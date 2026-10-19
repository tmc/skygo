package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func TestRetentionRawLegacyRequestIdentity(t *testing.T) {
	dir := t.TempDir()
	j := legacyFixture(t, dir, false)
	id := bytes.Repeat([]byte{0xff, 0, '<'}, bolt.MaxKeySize/3+1)[:bolt.MaxKeySize]
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		b := tx.Bucket(requestsBucket)
		if e := b.Delete([]byte("original-request")); e != nil {
			return e
		}
		return b.Put(id, []byte(j.ID))
	})
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	replay, e := s.Submit(testActor(), string(id), j.Task, "unused", "unused")
	if e != nil || replay.ID != j.ID {
		t.Fatalf("legacy replay %v %v", replay.ID, e)
	}
	if e = s.db.View(func(tx *bolt.Tx) error {
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
			return e
		}
		if !bytes.Equal(m.LegacyRequestID, id) || m.RequestID != "" {
			t.Fatal("legacy bytes normalized")
		}
		key := requestKey(m.Principal, m.Domain, m.requestID())
		if len(key) != 64 {
			t.Fatal("unbounded request key")
		}
		var r requestRecord
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), key, &r); e != nil {
			return e
		}
		if !bytes.Equal(r.LegacyRequestID, id) {
			t.Fatal("record identity normalized")
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	info, e := EnableRetention(dir, DefaultRetentionPolicy())
	if e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	replay, e = s.Submit(testActor(), string(id), j.Task, "unused", "unused")
	if e != nil || replay.ID != j.ID {
		t.Fatalf("enabled legacy replay %v", e)
	}
	if e = s.MaintainRetention(info.LegacyCutoff.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(requestsBucket).Get(id) != nil {
			t.Fatal("raw binding not retired")
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Get(j.ID); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionNewDisabledInvalidRequestRefusedUnchanged(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	for _, id := range []string{"", strings.Repeat("a", 129), "invalid\xff", "new id"} {
		before, e := os.ReadFile(s.db.Path())
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Submit(testActor(), id, backend.Task{Argv: []string{"work"}}, "local", "test-binding"); e == nil {
			t.Fatalf("accepted %q", id)
		}
		after, e := os.ReadFile(s.db.Path())
		if e != nil || !bytes.Equal(before, after) {
			t.Fatalf("invalid ID mutated DB %v", e)
		}
	}
}

func TestRetentionLegacyBindingGrowthChargedOnce(t *testing.T) {
	dir := t.TempDir()
	j := legacyFixture(t, dir, false)
	j.Status.Reason = strings.Repeat("x", 24000)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if e := putJSON(tx.Bucket(jobsBucket), []byte(j.ID), j); e != nil {
			return e
		}
		return tx.Bucket(metadataBucket).Put([]byte("owner"), []byte(strings.Repeat("a", 32)))
	})
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = EnableRetention(dir, DefaultRetentionPolicy()); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		u, e := usage(tx)
		if e != nil {
			return e
		}
		st.Policy.MetadataBytes = u[MetadataBytesQuota] - 1
		return putJSON(retentionRoot(tx), retentionStateKey, st)
	}); e != nil {
		t.Fatal(e)
	}
	binding, e := backend.LocalBinding(s.Owner())
	if e != nil {
		t.Fatal(e)
	}
	if e = s.BindLegacyLocal(binding); e != nil {
		t.Fatal(e)
	}
	first := routingState(t, s.db)
	if e = s.BindLegacyLocal(binding); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(first, routingState(t, s.db)) {
		t.Fatal("idempotent binding changed logical state")
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
	got, e := s.Get(j.ID)
	if e != nil || got.TargetBinding != binding || got.Status.Reason != j.Status.Reason {
		t.Fatalf("binding changed payload %v", e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(s.db.Path())
	if e != nil {
		t.Fatal(e)
	}
	if e = s.BindLegacyLocal("wrong"); e == nil {
		t.Fatal("foreign binding accepted")
	}
	after, e := os.ReadFile(s.db.Path())
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("binding refusal mutated %v", e)
	}
}

func TestRetentionScopedActivationChargesDuplicateRawBinding(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	j, e := s.Submit(testActor(), "legacy", backend.Task{Argv: []string{"work"}}, "local", "test-binding")
	if e != nil {
		t.Fatal(e)
	}
	var prior uint64
	if e = s.db.View(func(tx *bolt.Tx) error {
		var r requestRecord
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey("legacy", RequestDomain{}, "legacy"), &r); e != nil {
			return e
		}
		prior = r.LegacyBytes
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.ActivateScoped(); e != nil {
		t.Fatal(e)
	}
	if e = s.db.View(func(tx *bolt.Tx) error {
		var r requestRecord
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey("legacy", RequestDomain{}, "legacy"), &r); e != nil {
			return e
		}
		if r.LegacyBytes != 2*prior || r.LegacyBytes != uint64(2*(len("legacy")+len(j.ID))) {
			t.Fatalf("wrong duplicate charge %d", r.LegacyBytes)
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionLegacyRetirementStopsAtByteBudget(t *testing.T) {
	dir := t.TempDir()
	original := legacyFixture(t, dir, false)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if e := tx.Bucket(requestsBucket).Delete([]byte("original-request")); e != nil {
			return e
		}
		for i := 0; i < 20; i++ {
			j := original
			j.ID = fmt.Sprintf("%032x", i+1)
			j.Sequence = uint64(i + 1)
			if e := putJSON(tx.Bucket(jobsBucket), []byte(j.ID), j); e != nil {
				return e
			}
			if e := tx.Bucket(sequenceBucket).Put(sequenceKey(j.Sequence), []byte(j.ID)); e != nil {
				return e
			}
			id := bytes.Repeat([]byte{0xff}, bolt.MaxKeySize)
			id[len(id)-1] = byte(i)
			if e := tx.Bucket(requestsBucket).Put(id, []byte(j.ID)); e != nil {
				return e
			}
		}
		if e := tx.Bucket(jobsBucket).Delete([]byte(original.ID)); e != nil {
			return e
		}
		return tx.Bucket(sequenceBucket).SetSequence(20)
	})
	info, e := EnableRetention(dir, DefaultRetentionPolicy())
	if e != nil {
		t.Fatal(e)
	}
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.MaintainRetention(info.LegacyCutoff.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	remaining := 0
	if e = s.db.View(func(tx *bolt.Tx) error {
		remaining = retentionTable(tx, retainedRequestsBucket).Stats().KeyN
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
	if remaining <= 0 || remaining >= 20 {
		t.Fatalf("byte cap did not bound retirement: %d", remaining)
	}
	for i := 0; i < 10 && remaining > 0; i++ {
		if e = s.MaintainRetention(info.LegacyCutoff.Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if e = s.db.View(func(tx *bolt.Tx) error {
			remaining = retentionTable(tx, retainedRequestsBucket).Stats().KeyN
			return validateSnapshot(tx)
		}); e != nil {
			t.Fatal(e)
		}
	}
	if remaining != 0 {
		t.Fatal("retirement cursor stalled")
	}
}

// Encoding JSON is deliberately unrelated to raw request-key canonicalization.
func TestRetentionRawHashDistinguishesInvalidUTF8(t *testing.T) {
	a, b := string([]byte{0xff}), string([]byte{0xfe})
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if !bytes.Equal(ja, jb) {
		t.Fatal("fixture must collide under JSON normalization")
	}
	if bytes.Equal(requestKey("legacy", RequestDomain{}, a), requestKey("legacy", RequestDomain{}, b)) {
		t.Fatal("raw identities collided")
	}
}

func TestRetentionLegacyBindingAllowanceRefusesOtherChanges(t *testing.T) {
	dir := t.TempDir()
	j := legacyFixture(t, dir, false)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		return tx.Bucket(metadataBucket).Put([]byte("owner"), []byte(strings.Repeat("b", 32)))
	})
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	binding, e := backend.LocalBinding(s.Owner())
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(s.db.Path())
	if e != nil {
		t.Fatal(e)
	}
	e = s.db.Update(func(tx *bolt.Tx) error {
		changed := j
		changed.TargetBinding = binding
		changed.Status.Reason = "unrelated mutation"
		return putLegacyLocalBinding(tx, changed, binding)
	})
	if e == nil {
		t.Fatal("binding allowance changed status")
	}
	e = s.db.Update(func(tx *bolt.Tx) error {
		changed := j
		changed.TargetBinding = binding
		if e := putLegacyLocalBinding(tx, changed, binding); e != nil {
			return e
		}
		return putLegacyLocalBinding(tx, changed, binding)
	})
	if e == nil {
		t.Fatal("binding allowance repeated in same transaction")
	}
	after, e := os.ReadFile(s.db.Path())
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("migration refusal did not roll back %v", e)
	}
}
func TestRetentionLegacyBindingIgnoresNonlocalAndNewPinnedRows(t *testing.T) {
	dir := t.TempDir()
	j := legacyFixture(t, dir, false)
	j.Target = "remote"
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if e := putJSON(tx.Bucket(jobsBucket), []byte(j.ID), j); e != nil {
			return e
		}
		return tx.Bucket(metadataBucket).Put([]byte("owner"), []byte(strings.Repeat("c", 32)))
	})
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	binding, e := backend.LocalBinding(s.Owner())
	if e != nil {
		t.Fatal(e)
	}
	before := routingState(t, s.db)
	if e = s.BindLegacyLocal(binding); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(before, routingState(t, s.db)) {
		t.Fatal("nonlocal work migrated")
	}
	s.Close()
	enabled, info := enableTest(t, DefaultRetentionPolicy())
	p, _, e := enabled.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = enabled.SubmitRetained(testActor(), info.Domain, "modern", backend.Task{Argv: []string{"work"}}, "local", "modern-binding", backend.LocalOutput, uint64(backend.MaxLogBytes)+4*p.OutputUnit); e != nil {
		t.Fatal(e)
	}
	binding, e = backend.LocalBinding(enabled.Owner())
	if e != nil {
		t.Fatal(e)
	}
	before = routingState(t, enabled.db)
	if e = enabled.BindLegacyLocal(binding); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(before, routingState(t, enabled.db)) {
		t.Fatal("modern pinned work migrated")
	}
}

func TestRetentionRequestReinsertCannotDoubleCharge(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "one")
	before, e := os.ReadFile(s.db.Path())
	if e != nil {
		t.Fatal(e)
	}
	e = s.db.Update(func(tx *bolt.Tx) error {
		var m retainedJob
		var r requestRecord
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
			return e
		}
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(m.Principal, m.Domain, m.requestID()), &r); e != nil {
			return e
		}
		return trackJob(tx, j, m, r)
	})
	if e == nil {
		t.Fatal("existing identity inserted twice")
	}
	after, e := os.ReadFile(s.db.Path())
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("insertion guard mutated DB %v", e)
	}
}
func TestRetentionRequestKeyCannotReplaceIdentity(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	retainedSubmit(t, s, info.Domain, "one")
	before, e := os.ReadFile(s.db.Path())
	if e != nil {
		t.Fatal(e)
	}
	e = s.db.Update(func(tx *bolt.Tx) error {
		key := requestKey(testActor().Principal(), info.Domain, "one")
		var r requestRecord
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), key, &r); e != nil {
			return e
		}
		r.RequestID = "two"
		// Deliberate conflicting bucket contents exercise the all-writer identity
		// guard. This is not a generated SHA256 collision or a native qualification.
		return putRetentionRecord(tx, retainedRequestsBucket, key, r)
	})
	if e == nil {
		t.Fatal("hashed key replaced unrelated raw identity")
	}
	after, e := os.ReadFile(s.db.Path())
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("identity refusal mutated DB %v", e)
	}
}

func TestRetentionHistoricalScopedMirrorsAreOptionalAndExact(t *testing.T) {
	// The unchanged genuine schema3 fixture contains queued/running root mirrors,
	// plus a later scoped legacy request with no root copy. Both shapes are valid.
	dir := routingFixture(t, "schema3-scoped")
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	for _, tc := range []struct {
		id     string
		copies uint64
	}{{"queued", 2}, {"running", 2}, {"scoped", 1}} {
		if e = s.db.View(func(tx *bolt.Tx) error {
			var r requestRecord
			if e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey("legacy", RequestDomain{}, tc.id), &r); e != nil {
				return e
			}
			want := tc.copies * uint64(len(tc.id)+len(r.JobID))
			if r.LegacyBytes != want {
				t.Fatalf("%s charge=%d want%d", tc.id, r.LegacyBytes, want)
			}
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	j, e := s.Submit(testActor(), "new-scoped-legacy", backend.Task{Argv: []string{"work"}}, "local", "test-binding")
	if e != nil {
		t.Fatal(e)
	}
	if e = s.db.View(func(tx *bolt.Tx) error {
		id := "new-scoped-legacy"
		if tx.Bucket(requestsBucket).Get([]byte(id)) != nil {
			t.Fatal("new legacy root mirror invented")
		}
		var r requestRecord
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey("legacy", RequestDomain{}, id), &r); e != nil {
			return e
		}
		if r.LegacyBytes != uint64(len(id)+len(j.ID)) {
			t.Fatalf("new scoped request overcharged %d", r.LegacyBytes)
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionRetirementCursorAcceptsEveryHexPrefix(t *testing.T) {
	s, _ := enableTest(t, DefaultRetentionPolicy())
	for _, c := range "0123456789abcdef" {
		cursor := string(c) + strings.Repeat("0", 63)
		if e := s.db.Update(func(tx *bolt.Tx) error {
			st, e := loadRetention(tx)
			if e != nil {
				return e
			}
			st.RequestAfter = cursor
			return putJSON(retentionRoot(tx), retentionStateKey, st)
		}); e != nil {
			t.Fatal(e)
		}
		if e := s.db.View(validateSnapshot); e != nil {
			t.Fatalf("valid %c-leading cursor refused: %v", c, e)
		}
	}
	for _, cursor := range []string{"0", strings.Repeat("0", 65), "g" + strings.Repeat("0", 63), "A" + strings.Repeat("0", 63), strings.Repeat("-", 64)} {
		if e := s.db.Update(func(tx *bolt.Tx) error {
			st, e := loadRetention(tx)
			if e != nil {
				return e
			}
			st.RequestAfter = cursor
			return putJSON(retentionRoot(tx), retentionStateKey, st)
		}); e != nil {
			t.Fatal(e)
		}
		if e := s.db.View(validateSnapshot); e == nil {
			t.Fatalf("invalid retirement cursor accepted %q", cursor)
		}
	}
	// Leave the test store valid; invalid state was injected only for validation.
	if e := s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		st.RequestAfter = ""
		return putJSON(retentionRoot(tx), retentionStateKey, st)
	}); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionHistoricalMirrorLifecycleEnableRetireReopen(t *testing.T) {
	dir := routingFixture(t, "schema3-scoped")
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	var original []byte
	if e = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(jobsBucket).ForEach(func(k, v []byte) error {
			original = append(original, k...)
			original = append(original, v...)
			return nil
		})
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	info, e := EnableRetention(dir, DefaultRetentionPolicy())
	if e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.MaintainRetention(info.LegacyCutoff.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(requestsBucket).Stats().KeyN != 0 || tx.Bucket(principalRequestsBucket).Bucket([]byte("legacy")).Stats().KeyN != 0 || retentionTable(tx, retainedRequestsBucket).Stats().KeyN != 0 {
			t.Fatal("actual legacy mirror/nonmirror bindings not retired")
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.db.View(func(tx *bolt.Tx) error {
		var rows []byte
		if e := tx.Bucket(jobsBucket).ForEach(func(k, v []byte) error { rows = append(rows, k...); rows = append(rows, v...); return nil }); e != nil {
			return e
		}
		if !bytes.Equal(rows, original) {
			t.Fatal("legacy lifecycle changed accepted rows")
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
	_, e = s.Submit(testActor(), "scoped", backend.Task{Argv: []string{"unused"}}, "unused", "unused")
	expiryIs(t, e, RequestEpochExpired)
}
