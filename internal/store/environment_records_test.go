package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/environment"
)

func environmentTestRecord() environmentRecord {
	now := time.Date(2026, 10, 22, 1, 2, 3, 4, time.UTC)
	return environmentRecord{
		Identity:   environment.Identity{Installation: strings.Repeat("a", 32), ID: strings.Repeat("b", 32), Generation: 1},
		Principal:  "alice",
		RequestID:  "create-1",
		Spec:       strings.Repeat("c", 64),
		Binding:    "test-binding",
		Authority:  environmentDelegation("alice"),
		Ordinal:    1,
		Scope:      1,
		Created:    now,
		Updated:    now,
		Revision:   1,
		State:      environment.State{Desired: environment.Present, Phase: environment.Creating},
		Checkpoint: environmentCheckpoint("c"),
	}
}

func TestEnvironmentRecordCodec(t *testing.T) {
	r := environmentTestRecord()
	raw, err := encodeEnvironmentRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeEnvironmentRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := encodeEnvironmentRecord(got)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("round trip: %s %v", again, err)
	}
	s := string(raw)
	for _, test := range []struct {
		name, raw string
	}{
		{"missing", ""},
		{"null", "null"},
		{"space", " " + s},
		{"trailing", s + "{}"},
		{"unknown field", strings.Replace(s, `{"identity"`, `{"session":1,"identity"`, 1)},
		{"reordered", strings.Replace(s, `"principal":"alice","request_id":"create-1"`, `"request_id":"create-1","principal":"alice"`, 1)},
		{"noncanonical base64", strings.Replace(s, `"data":"Yw=="`, `"data":"Yw"`, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			var in []byte
			if test.raw != "" {
				in = []byte(test.raw)
			}
			if _, err := decodeEnvironmentRecord(in); err == nil {
				t.Fatalf("decoded %q", test.raw)
			}
		})
	}
	if _, err := decodeEnvironmentRecord(nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing record: %v", err)
	}
}

func TestEnvironmentRecordInvariants(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*environmentRecord)
	}{
		{"generation", func(r *environmentRecord) { r.Identity.Generation = 2 }},
		{"id", func(r *environmentRecord) { r.Identity.ID = "B" + r.Identity.ID[1:] }},
		{"installation", func(r *environmentRecord) { r.Identity.Installation = "" }},
		{"principal", func(r *environmentRecord) { r.Principal = "Alice" }},
		{"request", func(r *environmentRecord) { r.RequestID = "" }},
		{"spec digest", func(r *environmentRecord) { r.Spec = strings.Repeat("C", 64) }},
		{"binding", func(r *environmentRecord) { r.Binding = "" }},
		{"authority", func(r *environmentRecord) { r.Authority = environmentDelegation("bob") }},
		{"ordinal", func(r *environmentRecord) { r.Ordinal = 0 }},
		{"scope", func(r *environmentRecord) { r.Scope = 0 }},
		{"scope limit", func(r *environmentRecord) { r.Scope = maxPrincipalEnvironments + 1 }},
		{"revision", func(r *environmentRecord) { r.Revision = 0 }},
		{"created", func(r *environmentRecord) { r.Created, r.Updated = time.Time{}, time.Time{} }},
		{"updated before created", func(r *environmentRecord) { r.Updated = r.Created.Add(-time.Nanosecond) }},
		{"desired", func(r *environmentRecord) { r.State.Desired = 0 }},
		{"phase", func(r *environmentRecord) { r.State.Phase = 0 }},
		{"present deleted", func(r *environmentRecord) { r.State.Phase = environment.Deleted }},
		{"checkpoint kind", func(r *environmentRecord) { r.Checkpoint.Kind = "" }},
		{"checkpoint version", func(r *environmentRecord) { r.Checkpoint.Version = 0 }},
		{"checkpoint empty", func(r *environmentRecord) { r.Checkpoint.Data = nil }},
		{"checkpoint size", func(r *environmentRecord) { r.Checkpoint.Data = make([]byte, environment.MaxCheckpointBytes+1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := environmentTestRecord()
			test.edit(&r)
			if _, err := encodeEnvironmentRecord(r); err == nil {
				t.Fatal("encoded invalid record")
			}
			// A record written by other means is refused when read.
			raw, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeEnvironmentRecord(raw); err == nil {
				t.Fatal("decoded invalid record")
			}
		})
	}
}

func TestEnvironmentRecordLimitFitsMaximalRecord(t *testing.T) {
	r := environmentTestRecord()
	r.Identity.Installation = strings.Repeat("<", 128)
	r.Principal = "a" + strings.Repeat("b", 63)
	r.Authority = environmentDelegation(r.Principal)
	r.RequestID = strings.Repeat("r", 128)
	r.Binding = strings.Repeat("\x01", 256)
	r.Ordinal, r.Revision = ^uint64(0), ^uint64(0)
	r.Scope = maxPrincipalEnvironments
	r.State = environment.State{Desired: environment.Absent, Phase: environment.Deleting}
	r.Checkpoint = environment.Checkpoint{Kind: strings.Repeat("k", 63), Version: ^uint32(0), Data: make([]byte, environment.MaxCheckpointBytes)}
	raw, err := encodeEnvironmentRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("maximal record=%d cap=%d", len(raw), environmentRecordLimit)
}

func TestEnvironmentCharge(t *testing.T) {
	const limit = maxEnvironmentMetadataBytes
	for _, test := range []struct {
		name                  string
		usage, removed, added uint64
		ok                    bool
	}{
		{"empty", 0, 0, 0, true},
		{"fill", 0, 0, limit, true},
		{"over", 0, 0, limit + 1, false},
		{"exact", limit - 10, 0, 10, true},
		{"one over", limit - 10, 0, 11, false},
		{"replace at bound", limit, 10, 10, true},
		{"grow at bound", limit, 10, 11, false},
		{"shrink at bound", limit, 10, 9, true},
		{"removed exceeds usage", 5, 6, 0, false},
		{"usage over bound", limit + 1, 0, 0, false},
		{"usage over bound shrinking", limit + 2, 2, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := environmentCharge(test.usage, test.removed, test.added)
			if (err == nil) != test.ok || err != nil && !errors.Is(err, ErrEnvironmentLimit) {
				t.Fatalf("charge = %v, want ok %v", err, test.ok)
			}
		})
	}
}

func TestEnvironmentKeys(t *testing.T) {
	for _, test := range []struct {
		got  []byte
		want string
	}{
		{environmentRecordKey("id"), "e/id"},
		{environmentSpecKey("id"), "s/id"},
		{environmentRequestKey("alice", "req"), "r/alice/req"},
		{environmentNameKey("alice", "name"), "n/alice/name"},
		{environmentAliasKey("scratch", "kube/a"), "a/scratch/kube/a"},
		{environmentIndexKey(1), "i/\x00\x00\x00\x00\x00\x00\x00\x01"},
		{environmentHighKey("alice"), "h/alice"},
		{environmentScopeKey("alice", 2), "p/alice/\x00\x00\x00\x00\x00\x00\x00\x02"},
	} {
		if string(test.got) != test.want {
			t.Errorf("key %q, want %q", test.got, test.want)
		}
	}
}
