package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

type legacyRetirementDiskKey struct{ bucket, key string }
type legacyRetirementDisk struct {
	rows      map[legacyRetirementDiskKey][]byte
	sequences map[string]uint64
}

// Read actual stored bytes, including unrelated buckets, without consulting a
// retentionMutation, its changedBytes method, or its planned changes map.
func legacyRetirementReadDisk(t *testing.T, tx *bolt.Tx) legacyRetirementDisk {
	t.Helper()
	out := legacyRetirementDisk{map[legacyRetirementDiskKey][]byte{}, map[string]uint64{}}
	var walk func(string, *bolt.Bucket) error
	walk = func(path string, b *bolt.Bucket) error {
		out.sequences[path] = b.Sequence()
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return walk(path+"/"+string(k), b.Bucket(k))
			}
			out.rows[legacyRetirementDiskKey{path, string(k)}] = bytes.Clone(v)
			return nil
		})
	}
	if err := tx.ForEach(func(k []byte, b *bolt.Bucket) error { return walk(string(k), b) }); err != nil {
		t.Fatal(err)
	}
	return out
}
func legacyRetirementObserve(t *testing.T, s *Store) legacyRetirementDisk {
	t.Helper()
	var out legacyRetirementDisk
	if err := s.db.View(func(tx *bolt.Tx) error { out = legacyRetirementReadDisk(t, tx); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

// The ledger counts changed key+old+new values. Usage is an unconditional
// scheduled write, so charge it once even if equal. State is charged only if
// its stored encoding changes. Maintenance must not alter bucket sequences or
// create/remove buckets; fail instead of silently granting another allowance.
func legacyRetirementObservedBytes(t *testing.T, before, after legacyRetirementDisk) uint64 {
	t.Helper()
	if len(before.sequences) != len(after.sequences) {
		t.Fatal("maintenance changed bucket inventory")
	}
	for path, seq := range before.sequences {
		if next, ok := after.sequences[path]; !ok || next != seq {
			t.Fatal("maintenance changed permanent bucket sequence", path, seq, next)
		}
	}
	keys := map[legacyRetirementDiskKey]bool{}
	for key := range before.rows {
		keys[key] = true
	}
	for key := range after.rows {
		keys[key] = true
	}
	usageKey := legacyRetirementDiskKey{string(retentionBucket), string(retentionUsageKey)}
	if before.rows[usageKey] == nil || after.rows[usageKey] == nil {
		t.Fatal("observed ledger lacks actual usage record")
	}
	var n uint64
	for key := range keys {
		if key != usageKey && bytes.Equal(before.rows[key], after.rows[key]) {
			continue
		}
		n += uint64(len(key.key) + len(before.rows[key]) + len(after.rows[key]))
	}
	return n
}

// These inputs must be produced by the unchanged archived public Store APIs,
// separately built/reviewed by the parent. No private-row fixture synthesis is
// accepted as the archive/prototype proof in this opt-in lane.
type legacyRetirementArchive struct {
	Origin          string `json:"origin"`
	SourceSHA256    string `json:"source_sha256"`
	DatabaseSHA256  string `json:"database_sha256"`
	Scoped          bool   `json:"scoped"`
	AlreadyRetired  bool   `json:"already_retired"`
	ExpectedRows    int    `json:"expected_rows"`
	ExpectedRawSize int    `json:"expected_raw_size"`
}

func legacyRetirementArchiveInput(t *testing.T, env string, prototype bool) *legacyRetirementFixture {
	t.Helper()
	manifest := os.Getenv(env)
	if manifest == "" {
		t.Skip("requires parent-pinned genuine " + env)
	}
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if len(raw) > 1<<20 || os.Getenv(env+"_SHA256") != hex.EncodeToString(sum[:]) {
		t.Fatal("genuine fixture manifest size/hash mismatch")
	}
	var input legacyRetirementArchive
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("genuine fixture manifest has trailing data")
	}
	if input.ExpectedRows < 8 || input.ExpectedRows > 64 || input.ExpectedRawSize != bolt.MaxKeySize || input.AlreadyRetired != prototype {
		t.Fatal("genuine fixture has wrong finite corpus shape")
	}
	wantOrigin := "s02-public-store"
	wantSource := "c35dbb7852e51d830a1e92eb2d6be6ea13860944768eb8a8b35ebaa66fb63f19"
	if prototype {
		wantOrigin = "c11-public-maintenance"
		wantSource = "8ea220ab1d774de6caf99d00a7e0fe3b10e9f386c6eb78e037abfdccaad18e3a"
	}
	if input.Origin != wantOrigin || input.SourceSHA256 != wantSource {
		t.Fatal("genuine fixture source authority differs")
	}
	path := filepath.Join(filepath.Dir(manifest), "jobs.db")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 256<<20 {
		t.Fatal("genuine fixture must be a bounded regular stopped database")
	}
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	f := &legacyRetirementFixture{dir: t.TempDir()}
	dest, err := os.OpenFile(filepath.Join(f.dir, "jobs.db"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	copied, copyErr := io.Copy(io.MultiWriter(dest, hash), io.LimitReader(source, (256<<20)+1))
	syncErr, closeErr := dest.Sync(), dest.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || copied != info.Size() || hex.EncodeToString(hash.Sum(nil)) != input.DatabaseSHA256 {
		t.Fatal("genuine fixture copy/hash failed", copyErr, syncErr, closeErr)
	}
	f.s, err = Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.s != nil {
			if err := f.s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if f.s.Scoped() != input.Scoped {
		t.Fatal("genuine namespace shape changed")
	}
	if !prototype {
		f.close(t)
		f.info, err = EnableRetention(f.dir, DefaultRetentionPolicy())
		if err != nil {
			t.Fatal(err)
		}
		f.open(t)
	} else {
		f.info, err = f.s.RetentionInfo(testActor())
		if err != nil || !f.info.Enabled || f.info.LegacyReplayOpen {
			t.Fatal("prototype did not commit original closure", err)
		}
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(jobsBucket).ForEach(func(id, _ []byte) error {
			j, err := get(tx, string(id))
			if err != nil {
				return err
			}
			var m retainedJob
			if err := loadJSON(retentionTable(tx, retainedJobsBucket), id, &m); err != nil {
				return err
			}
			if !m.Legacy || m.LegacyRequestRetired || len(m.LegacyRequestID) != input.ExpectedRawSize || bytes.IndexByte(m.LegacyRequestID, 0xff) < 0 || bytes.IndexByte(m.LegacyRequestID, 0) < 0 {
				return errors.New("genuine maximum invalid-byte legacy identity missing")
			}
			f.jobs, f.ids = append(f.jobs, j), append(f.ids, string(bytes.Clone(m.LegacyRequestID)))
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(f.jobs) != input.ExpectedRows {
		t.Fatal("genuine fixture row count differs")
	}
	if prototype {
		if err := f.s.db.View(func(tx *bolt.Tx) error {
			if retentionTable(tx, retainedRequestsBucket).Stats().KeyN != 0 {
				return errors.New("prototype requests were not actually collected")
			}
			return validateSnapshot(tx)
		}); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func TestLegacyIdentityRetirementGenuineMaximumArchive(t *testing.T) {
	f := legacyRetirementArchiveInput(t, "SKYGO_LEGACY_RETIREMENT_ARCHIVE", false)
	before := legacyRetirementCanonical(t, f.s)
	initial := legacyRetirementUsage(t, f.s)
	var saved uint64
	for _, j := range f.jobs {
		m, raw := legacyRetirementMeta(t, f.s, j.ID)
		m.LegacyRequestID, m.LegacyRequestRetired = nil, true
		next, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		saved += uint64(len(raw) - len(next))
	}
	for call := 0; call < 128; call++ {
		if err := f.s.MaintainRetention(f.info.LegacyCutoff); err != nil {
			t.Fatal(err)
		}
		if legacyRetirementUsage(t, f.s)[RequestRecordsQuota] == 0 {
			break
		}
	}
	after := legacyRetirementUsage(t, f.s)
	if after[RequestRecordsQuota] != 0 || initial[MetadataBytesQuota]-after[MetadataBytesQuota] != saved || !bytes.Equal(before, legacyRetirementCanonical(t, f.s)) {
		t.Fatal("maximum archived retirement failed exact side-copy release/canonical invariance")
	}
	for _, j := range f.jobs {
		m, _ := legacyRetirementMeta(t, f.s, j.ID)
		if !m.LegacyRequestRetired || m.LegacyRequestID != nil {
			t.Fatal("genuine maximum identity remains")
		}
	}
	f.close(t)
	f.open(t)
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyIdentityRetirementGenuinePrototypeFallback(t *testing.T) {
	f := legacyRetirementArchiveInput(t, "SKYGO_LEGACY_RETIREMENT_PROTOTYPE", true)
	before := legacyRetirementCanonical(t, f.s)
	initial := legacyRetirementUsage(t, f.s)
	var saved uint64
	for _, j := range f.jobs {
		m, old := legacyRetirementMeta(t, f.s, j.ID)
		m.LegacyRequestID, m.LegacyRequestRetired = nil, true
		next, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		saved += uint64(len(old) - len(next))
	}
	firstRefused := legacyRetirementPrototypeBoundary(t, f)

	// A maximum identity is far larger than a count-only cursor trial. Require
	// real bounded progress/reopen rather than fabricating Max or a nil key.
	for call := 1; call < 128; call++ {
		if err := f.s.MaintainRetention(time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		if call == 1 {
			m, _ := legacyRetirementMeta(t, f.s, firstRefused)
			if !m.LegacyRequestRetired || m.LegacyRequestID != nil {
				t.Fatal("first byte-refused fallback subject was skipped after reopen")
			}
		}
		left := 0
		for _, j := range f.jobs {
			m, _ := legacyRetirementMeta(t, f.s, j.ID)
			if !m.LegacyRequestRetired {
				left++
			}
		}
		if left == 0 {
			break
		}
		f.close(t)
		f.open(t)
	}
	after := legacyRetirementUsage(t, f.s)
	if initial[MetadataBytesQuota]-after[MetadataBytesQuota] != saved || after[RequestBytesQuota] != initial[RequestBytesQuota] || after[RequestRecordsQuota] != 0 || !bytes.Equal(before, legacyRetirementCanonical(t, f.s)) {
		t.Fatal("prototype fallback changed requests/canonical rows or failed release")
	}
	for _, j := range f.jobs {
		m, _ := legacyRetirementMeta(t, f.s, j.ID)
		if !m.LegacyRequestRetired {
			t.Fatal("bounded prototype pass never reached a live job")
		}
	}
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual prototype metadata release=%d", initial[MetadataBytesQuota]-after[MetadataBytesQuota])
}

func legacyRetirementPrototypeBoundary(t *testing.T, f *legacyRetirementFixture) string {
	t.Helper()
	type subject struct {
		sequence uint64
		id       string
	}
	var ordered []subject
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(sequenceBucket).ForEach(func(k, id []byte) error {
			if len(k) != 8 {
				return errors.New("invalid actual acceptance index")
			}
			ordered = append(ordered, subject{binary.BigEndian.Uint64(k), string(id)})
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(ordered) < 32 || len(ordered) > 64 {
		t.Fatal("boundary requires actual maximum32..64 job profile")
	}
	before := legacyRetirementObserve(t, f.s)
	if err := f.s.MaintainRetention(time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	after := legacyRetirementObserve(t, f.s)
	observed := legacyRetirementObservedBytes(t, before, after)
	if observed > 1<<20 {
		t.Fatal("actual fallback maintenance exceeded1MiB", observed)
	}
	accepted := 0
	for _, item := range ordered {
		m, _ := legacyRetirementMeta(t, f.s, item.id)
		if m.LegacyRequestRetired {
			if accepted >= len(ordered) || ordered[accepted].id != item.id {
				t.Fatal("fallback skipped an earlier actual index subject")
			}
			accepted++
		}
	}
	if accepted == 0 || accepted == len(ordered) {
		t.Fatal("maximum fallback did not expose a genuine byte refusal", accepted)
	}
	stateKey := legacyRetirementDiskKey{string(retentionBucket), string(retentionStateKey)}
	usageKey := legacyRetirementDiskKey{string(retentionBucket), string(retentionUsageKey)}
	var st retentionState
	if err := json.Unmarshal(after.rows[stateKey], &st); err != nil {
		t.Fatal(err)
	}
	if st.LegacyJobsAfter != ordered[accepted-1].sequence || st.LegacyJobsMax != ordered[len(ordered)-1].sequence {
		t.Fatal("fallback cursor is not exactly last accepted trial and captured high-water", st.LegacyJobsAfter, st.LegacyJobsMax)
	}
	refused := ordered[accepted]
	metaKey := legacyRetirementDiskKey{string(retentionBucket) + "/" + string(retainedJobsBucket), refused.id}
	if !bytes.Equal(before.rows[metaKey], after.rows[metaKey]) {
		t.Fatalf("first byte-refused metadata changed: sequence=%d before_bytes=%d after_bytes=%d", refused.sequence, len(before.rows[metaKey]), len(after.rows[metaKey]))
	}
	var meta retainedJob
	if err := json.Unmarshal(after.rows[metaKey], &meta); err != nil {
		t.Fatal(err)
	}
	if meta.LegacyRequestRetired || len(meta.LegacyRequestID) != bolt.MaxKeySize {
		t.Fatal("first refused subject was changed")
	}
	meta.LegacyRequestID, meta.LegacyRequestRetired = nil, true
	encoded, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	var used retentionUsage
	if err := json.Unmarshal(after.rows[usageKey], &used); err != nil {
		t.Fatal(err)
	}
	used[MetadataBytesQuota] -= uint64(len(after.rows[metaKey]) - len(encoded))
	st.LegacyJobsAfter = refused.sequence
	// Independently form just the next legal side-record/cursor/usage encoding.
	// There are <100 real subjects and no requests, so the next trial can be
	// refused only by bytes. It must exceed the same ORIGINAL transaction bound.
	hypothetical := legacyRetirementDisk{map[legacyRetirementDiskKey][]byte{}, after.sequences}
	for key, value := range after.rows {
		hypothetical.rows[key] = value
	}
	hypothetical.rows[metaKey] = encoded
	hypothetical.rows[usageKey], err = json.Marshal(used)
	if err != nil {
		t.Fatal(err)
	}
	hypothetical.rows[stateKey], err = json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if next := legacyRetirementObservedBytes(t, before, hypothetical); next <= 1<<20 {
		t.Fatal("cursor stopped without an independently demonstrated byte refusal", observed, next)
	}
	f.close(t)
	f.open(t)
	reopened := legacyRetirementObserve(t, f.s)
	if !bytes.Equal(after.rows[stateKey], reopened.rows[stateKey]) || !bytes.Equal(after.rows[metaKey], reopened.rows[metaKey]) {
		t.Fatal("reopen advanced byte-refused subject or changed accepted cursor")
	}
	t.Logf("actual fallback accepted=%d observed_bytes=%d first_refused_sequence=%d", accepted, observed, refused.sequence)
	return refused.id
}

func TestLegacyIdentityRetirementMaximumPlanLedger(t *testing.T) {
	f := legacyRetirementArchiveInput(t, "SKYGO_LEGACY_RETIREMENT_ARCHIVE", false)
	rollback := routingState(t, f.s.db)
	abort := errors.New("abort independently observed half-lane commit")
	if err := f.s.db.Update(func(tx *bolt.Tx) error {
		before := legacyRetirementReadDisk(t, tx)
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		advancePhases(&st, f.info.LegacyCutoff)
		m, err := newRetentionMutation(tx)
		if err != nil {
			return err
		}
		if err := m.rootPut(retentionStateKey, st); err != nil {
			return err
		}
		m, _, err = retireRequestLane(m, &st)
		if err != nil {
			return err
		}
		claimed, err := m.changedBytes()
		if err != nil {
			return err
		}
		if err := m.commit(); err != nil {
			return err
		}
		observed := legacyRetirementObservedBytes(t, before, legacyRetirementReadDisk(t, tx))
		if observed > 512<<10 || observed != claimed {
			return fmt.Errorf("observed half ledger=%d claimed=%d", observed, claimed)
		}
		return abort
	}); !errors.Is(err, abort) || !bytes.Equal(rollback, routingState(t, f.s.db)) {
		t.Fatal("observed request-only trial did not roll back exactly", err)
	}
	if err := f.s.db.Update(func(tx *bolt.Tx) error {
		before := legacyRetirementReadDisk(t, tx)
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		advancePhases(&st, f.info.LegacyCutoff)
		m, err := newRetentionMutation(tx)
		if err != nil {
			return err
		}
		if err := m.rootPut(retentionStateKey, st); err != nil {
			return err
		}
		m, requests, err := retireRequestLane(m, &st)
		if err != nil {
			return err
		}
		n, err := m.changedBytes()
		if err != nil || n > maxChangeBytes/2 || requests > 50 {
			return fmt.Errorf("maximum request lane subjects=%d ledger=%d err=%v", requests, n, err)
		}
		var lastDeleted string
		for key, value := range m.changes {
			if key.retention && key.table == string(retainedRequestsBucket) && value == nil && key.key > lastDeleted {
				lastDeleted = key.key
			}
		}
		if lastDeleted == "" || st.RequestAfter != lastDeleted {
			return errors.New("request budget advanced beyond the last accepted retirement")
		}
		m, jobs, err := retireLegacyJobLane(m, &st, 100-requests)
		if err != nil {
			return err
		}
		n, err = m.changedBytes()
		if err != nil || n > maxChangeBytes || jobs == 0 || jobs+requests > 100 {
			return fmt.Errorf("maximum shared plan subjects=%d ledger=%d err=%v", jobs+requests, n, err)
		}
		if err := m.commit(); err != nil {
			return err
		}
		observed := legacyRetirementObservedBytes(t, before, legacyRetirementReadDisk(t, tx))
		if observed > 1<<20 || observed != n {
			return fmt.Errorf("observed full ledger=%d claimed=%d", observed, n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Also measure the actual public maintenance entrypoint, independently of
	// the explicit two-lane test decomposition above.
	before := legacyRetirementObserve(t, f.s)
	if err := f.s.MaintainRetention(f.info.LegacyCutoff); err != nil {
		t.Fatal(err)
	}
	if observed := legacyRetirementObservedBytes(t, before, legacyRetirementObserve(t, f.s)); observed > 1<<20 {
		t.Fatal("actual public maintenance exceeded observed1MiB ledger", observed)
	}
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyIdentityRetirementGenuineOverQuota(t *testing.T) {
	f := legacyRetirementArchiveInput(t, "SKYGO_LEGACY_RETIREMENT_OVERQUOTA", false)
	before := legacyRetirementUsage(t, f.s)
	if before[MetadataBytesQuota] <= DefaultRetentionPolicy().MetadataBytes {
		t.Fatal("genuine long canonical status profile is not over default metadata quota")
	}
	canonical := legacyRetirementCanonical(t, f.s)
	if err := f.s.MaintainRetention(f.info.LegacyCutoff); err != nil {
		t.Fatal("non-growing legacy retirement refused while over quota", err)
	}
	after := legacyRetirementUsage(t, f.s)
	if after[MetadataBytesQuota] >= before[MetadataBytesQuota] || after[MetadataBytesQuota] <= DefaultRetentionPolicy().MetadataBytes || !bytes.Equal(canonical, legacyRetirementCanonical(t, f.s)) {
		t.Fatal("retirement changed canonical status or failed release while still over quota")
	}
	info, err := f.s.RetentionInfo(testActor())
	if err != nil {
		t.Fatal(err)
	}
	f.s.now = func() time.Time { return f.info.LegacyCutoff }
	state := routingState(t, f.s.db)
	_, err = f.s.SubmitRetained(testActor(), info.Domain, "no-positive-growth", backend.Task{Argv: []string{"never-executed"}}, "remote", "test-binding", backend.RemoteOutput, 0)
	var capacity *CapacityError
	if !errors.As(err, &capacity) || capacity.Domain != MetadataBytesQuota || capacity.Used != after[MetadataBytesQuota] || capacity.Limit != DefaultRetentionPolicy().MetadataBytes || !bytes.Equal(state, routingState(t, f.s.db)) {
		t.Fatal("over-quota positive growth gained an exception", err)
	}
}

func TestLegacyIdentityRetirementPrototypeRefusesRelations(t *testing.T) {
	for _, relation := range []string{"namespace_same", "namespace_conflict", "root_mirror", "request_conflict"} {
		t.Run(relation, func(t *testing.T) {
			f := legacyRetirementArchiveInput(t, "SKYGO_LEGACY_RETIREMENT_PROTOTYPE", true)
			j := f.jobs[0]
			meta, _ := legacyRetirementMeta(t, f.s, j.ID)
			if relation == "root_mirror" && !f.s.Scoped() {
				t.Skip("root mirror negative requires the mirrored prototype release profile")
			}
			// These are explicit corruptions of a genuine accepted prototype,
			// never synthesized positive identity/proof rows or a credit oracle.
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				if relation == "request_conflict" {
					digest, err := taskDigest(j.Task)
					if err != nil {
						return err
					}
					digest[0] ^= 1
					r := requestRecord{Principal: meta.Principal, Domain: meta.Domain, LegacyRequestID: meta.LegacyRequestID, JobID: j.ID, TaskDigest: digest}
					return putRetentionRecord(tx, retainedRequestsBucket, requestKey(meta.Principal, meta.Domain, meta.requestID()), r)
				}
				b := requestNamespace(tx, meta.Principal)
				if relation == "root_mirror" {
					b = tx.Bucket(requestsBucket)
				}
				value := []byte(j.ID)
				if relation == "namespace_conflict" {
					value = []byte(f.jobs[1].ID)
				}
				return b.Put(meta.LegacyRequestID, value)
			}); err != nil {
				t.Fatal(err)
			}
			before := routingState(t, f.s.db)
			if err := f.s.db.View(func(tx *bolt.Tx) error {
				st, err := loadRetention(tx)
				if err != nil {
					return err
				}
				m, err := newRetentionMutation(tx)
				if err != nil {
					return err
				}
				if err := m.retireLegacyJob(st, j); err == nil {
					return errors.New("fallback accepted a conflicting raw/request relation")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.s.MaintainRetention(time.Now().UTC()); err == nil || !bytes.Equal(before, routingState(t, f.s.db)) {
				t.Fatal("conflicting prototype relation was repaired, credited or partially committed", err)
			}
		})
	}
}
