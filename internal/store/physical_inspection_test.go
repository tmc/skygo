//go:build linux || darwin

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func physicalTestState(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}
func physicalTestDigest(t *testing.T, dir string) [32]byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(raw)
}

func TestPhysicalInspectionModern(t *testing.T) {
	dir := physicalTestState(t)
	before := physicalTestDigest(t, dir)
	probe, err := ProbePhysicalState(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if !probe.ProbeComplete || probe.GeometryStatus != "not_requested" || probe.ExaminedEntries != 0 || probe.Jobs != nil || probe.MigratedLivePayloadLowerBound != nil {
		t.Fatalf("probe=%+v", probe)
	}
	full, err := InspectPhysicalState(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if !full.ProbeComplete || full.GeometryStatus != "complete" || full.Jobs == nil || *full.Jobs != 0 || full.AddedLegacyIDPayloadLowerBound == nil || *full.AddedLegacyIDPayloadLowerBound != 0 {
		t.Fatalf("inspection=%+v", full)
	}
	if physicalTestDigest(t, dir) != before {
		t.Fatal("inspection wrote database")
	}
	// Reopening exclusively also checks that inspection released the shared lock.
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalInspectionMissingAndCanceled(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "canceled"}[cancelled], func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "absent", "state")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelled {
				cancel()
			}
			out, err := InspectPhysicalState(ctx, dir)
			if err == nil || out.ProbeComplete || out.Jobs != nil || out.GeometryStatus != "incomplete" {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			if cancelled && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation cause lost", err)
			}
			if _, err = os.Lstat(filepath.Dir(dir)); !os.IsNotExist(err) {
				t.Fatalf("missing parent created: %v", err)
			}
		})
	}
}

func TestPhysicalInspectionFileRefusals(t *testing.T) {
	for _, kind := range []string{"empty", "short", "directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "jobs.db")
			switch kind {
			case "empty", "short":
				n := 0
				if kind == "short" {
					n = 2047
				}
				if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, n), 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				source := filepath.Join(physicalTestState(t), "jobs.db")
				if err := os.Symlink(source, path); err != nil {
					t.Fatal(err)
				}
			}
			var before []byte
			if kind == "empty" || kind == "short" {
				before, _ = os.ReadFile(path)
			}
			db, borrowed, err := openPhysicalSnapshot(path, time.Millisecond)
			if err == nil || db != nil || borrowed != nil {
				if db != nil {
					db.Close()
				}
				t.Fatal("invalid source accepted/borrowed handle returned")
			}
			out, err := ProbePhysicalState(context.Background(), dir)
			if err == nil || out.ProbeComplete {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			if before != nil {
				after, e := os.ReadFile(path)
				if e != nil || !bytes.Equal(before, after) {
					t.Fatal("refusal changed source", e)
				}
			}
		})
	}
}

func TestPhysicalInspectionLocked(t *testing.T) {
	dir := physicalTestState(t)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := ProbePhysicalState(context.Background(), dir)
	if err == nil || probe.Reason != "locked" || probe.FileBytes != nil || probe.ProbeComplete {
		t.Fatalf("probe=%+v error=%v", probe, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Baseline after the actual writer's own open/close, not a claim it is readonly.
	afterWriter := physicalTestDigest(t, dir)
	if _, err = ProbePhysicalState(context.Background(), dir); err != nil {
		t.Fatal(err)
	}
	if physicalTestDigest(t, dir) != afterWriter {
		t.Fatal("probe changed database")
	}
}

func TestPhysicalInspectionScalarFence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		edit     func(*bolt.Tx) error
		probeErr bool
		reason   string
	}{
		{"owner-absent", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete([]byte("owner")) }, false, ""},
		{"owner-wide", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put([]byte("owner"), bytes.Repeat([]byte{'o'}, 65537))
		}, true, "owner_scalar_budget"},
		{"schema-zero", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(0)) }, true, "unsupported_schema"},
		{"future", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(6)) }, true, "unsupported_schema"},
		{"bad-marker", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, []byte{1}) }, true, "invalid_schema_scalar"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			legacyFixture(t, dir, false)
			editFixture(t, dir, false, tc.edit)
			before := physicalTestDigest(t, dir)
			out, err := ProbePhysicalState(context.Background(), dir)
			if (err != nil) != tc.probeErr || out.Reason != tc.reason || out.ExaminedEntries != 0 || out.GeometryStatus != "not_requested" {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			if tc.name == "owner-absent" {
				if out.OwnerPresent == nil || *out.OwnerPresent {
					t.Fatal("owner was invented")
				}
				full, e := InspectPhysicalState(context.Background(), dir)
				if e == nil || full.Reason != "missing_owner" || full.Jobs != nil {
					t.Fatalf("full=%+v err=%v", full, e)
				}
			}
			if physicalTestDigest(t, dir) != before {
				t.Fatal("scalar inspection wrote source")
			}
		})
	}
}

func TestPhysicalInspectionRetentionScalarPresence(t *testing.T) {
	for _, tc := range []struct {
		name           string
		raw            string
		known, enabled bool
	}{
		{"missing", `{}`, false, false},
		{"whole-null", `null`, false, false},
		{"enabled-null", `{"enabled":null}`, false, false},
		{"enabled-false", `{"enabled":false}`, true, false},
		{"enabled-true", `{"enabled":true}`, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := physicalTestState(t)
			editFixture(t, dir, false, func(tx *bolt.Tx) error {
				return tx.Bucket(retentionBucket).Put(retentionStateKey, []byte(tc.raw))
			})
			before := physicalTestDigest(t, dir)
			for _, geometry := range []bool{false, true} {
				var out PhysicalInspection
				var err error
				if geometry {
					out, err = InspectPhysicalState(context.Background(), dir)
				} else {
					out, err = ProbePhysicalState(context.Background(), dir)
				}
				if (err == nil) != tc.known || out.ProbeComplete != tc.known {
					t.Fatalf("geometry=%v out=%+v err=%v", geometry, out, err)
				}
				if tc.known {
					if out.RetentionEnabled == nil || *out.RetentionEnabled != tc.enabled {
						t.Fatalf("invented boolean: %+v", out)
					}
					raw, e := json.Marshal(out)
					want := []byte(`"retention_enabled":false`)
					if tc.enabled {
						want = []byte(`"retention_enabled":true`)
					}
					if e != nil || !bytes.Contains(raw, want) {
						t.Fatalf("known scalar JSON=%s err=%v", raw, e)
					}
				} else {
					if geometry && out.GeometryStatus != "incomplete" {
						t.Fatalf("unknown geometry completed: %+v", out)
					}
					if out.RetentionEnabled != nil || out.Reason != "invalid_retention_scalar" || out.Jobs != nil {
						t.Fatalf("unknown became observed: %+v", out)
					}
					raw, e := json.Marshal(out)
					if e != nil || bytes.Contains(raw, []byte(`"retention_enabled"`)) {
						t.Fatalf("unknown scalar JSON=%s err=%v", raw, e)
					}
				}
				if !geometry && out.ExaminedEntries != 0 {
					t.Fatal("cheap probe scanned records")
				}
				if physicalTestDigest(t, dir) != before {
					t.Fatal("scalar inspection wrote source")
				}
			}
			// A subsequent exclusive writer demonstrates both readonly calls
			// released their lock without trying to validate the altered state.
			db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPhysicalInspectionGeometryAndPrivacy(t *testing.T) {
	for _, shape := range []string{"root", "mirrored", "scoped-only", "conflicting-mirror"} {
		t.Run(shape, func(t *testing.T) {
			dir := t.TempDir()
			job := legacyFixture(t, dir, false)
			raw := bytes.Repeat([]byte{0xff}, 32768)
			copy(raw, []byte("private-request-sentinel"))
			editFixture(t, dir, false, func(tx *bolt.Tx) error {
				root := tx.Bucket(requestsBucket)
				if e := root.Delete([]byte("original-request")); e != nil {
					return e
				}
				if e := root.Put(raw, []byte(job.ID)); e != nil {
					return e
				}
				// Opaque canonical content proves this diagnostic does not decode Jobs.
				if e := tx.Bucket(jobsBucket).Put([]byte(job.ID), []byte("private-task-not-json")); e != nil {
					return e
				}
				if shape == "root" {
					return nil
				}
				owners, e := tx.CreateBucket(jobPrincipalsBucket)
				if e != nil {
					return e
				}
				if e = owners.Put([]byte(job.ID), []byte("legacy")); e != nil {
					return e
				}
				requests, e := tx.CreateBucket(principalRequestsBucket)
				if e != nil {
					return e
				}
				legacy, e := requests.CreateBucket([]byte("legacy"))
				if e != nil {
					return e
				}
				if e = legacy.Put(raw, []byte(job.ID)); e != nil {
					return e
				}
				if shape == "scoped-only" {
					if e = root.Delete(raw); e != nil {
						return e
					}
				}
				if shape == "conflicting-mirror" {
					if e = legacy.Put(raw, []byte("different-job")); e != nil {
						return e
					}
				}
				if e = tx.Bucket(metadataBucket).Put(scopedKey, []byte{1}); e != nil {
					return e
				}
				return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(2))
			})
			before := physicalTestDigest(t, dir)
			out, err := InspectPhysicalState(context.Background(), dir)
			if shape == "conflicting-mirror" {
				if err == nil || out.Jobs != nil {
					t.Fatal("conflict accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			copies := uint64(1)
			if shape == "mirrored" {
				copies = 2
			}
			if *out.Jobs != 1 || *out.DistinctLegacyBindings != 1 || *out.RawRequestKeyBytes != 32768*copies || *out.AddedLegacyIDPayloadLowerBound != 2*43692 {
				t.Fatalf("geometry=%+v", out)
			}
			encoded, e := json.Marshal(out)
			if e != nil {
				t.Fatal(e)
			}
			for _, sentinel := range []string{"private-request-sentinel", "private-task-not-json", "legacy-owner"} {
				if bytes.Contains(encoded, []byte(sentinel)) {
					t.Fatal("private value disclosed", sentinel)
				}
			}
			if physicalTestDigest(t, dir) != before {
				t.Fatal("geometry wrote/migrated source")
			}
		})
	}
}

func TestPhysicalInspectionBudgets(t *testing.T) {
	for _, kind := range []string{"entries", "input", "depth", "ids", "clones", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			legacyFixture(t, dir, false)
			limit := physicalLimits{duration: time.Second, lock: time.Millisecond, entries: 131072, input: 2 << 30, depth: 8, ids: 16384, clones: 16 << 20}
			switch kind {
			case "entries":
				limit.entries = 1
			case "input":
				limit.input = 1
			case "depth":
				limit.depth = 0
			case "ids":
				limit.ids = 0
			case "clones":
				limit.clones = 1
			case "deadline":
				limit.duration = time.Nanosecond
			}
			before := physicalTestDigest(t, dir)
			out, err := inspectPhysicalState(context.Background(), dir, true, limit)
			if err == nil || out.GeometryStatus != "incomplete" || out.Jobs != nil || out.MigratedLivePayloadLowerBound != nil {
				t.Fatalf("budget not enforced: %+v %v", out, err)
			}
			if physicalTestDigest(t, dir) != before {
				t.Fatal("budget refusal wrote source")
			}
		})
	}
}

func TestPhysicalInspectionDescriptorIdentity(t *testing.T) {
	dir := physicalTestState(t)
	path := filepath.Join(dir, "jobs.db")
	db, file, err := openPhysicalSnapshot(path, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var out PhysicalInspection
	before, err := physicalDescriptor(context.Background(), file, &out)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path, filepath.Join(dir, "original.db")); err != nil {
		t.Fatal(err)
	}
	other := physicalTestState(t)
	data, err := os.ReadFile(filepath.Join(other, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	last, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, last) {
		t.Fatal("replacement identity was adopted")
	}
	var same PhysicalInspection
	current, err := physicalDescriptor(context.Background(), file, &same)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, current) || *out.Inode != *same.Inode {
		t.Fatal("borrowed descriptor changed identity")
	}
}

func TestPhysicalInspectionPreservesMetadata(t *testing.T) {
	dir := physicalTestState(t)
	path := filepath.Join(dir, "jobs.db")
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, read := range []func(context.Context, string) (PhysicalInspection, error){ProbePhysicalState, InspectPhysicalState} {
		if _, err = read(context.Background(), dir); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || before.Mode() != after.Mode() || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("inspection changed source metadata")
	}
}
