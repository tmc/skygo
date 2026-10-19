//go:build linux || darwin

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

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// TestRetentionGenuineLegacyStress consumes only the separately source-pinned
// S02 public-API producer's stopped databases. It never produces old rows itself.
func TestRetentionGenuineLegacyStress(t *testing.T) {
	root := os.Getenv("SKYGO_LEGACY_STRESS_ROOT")
	if root == "" {
		t.Skip("requires source-pinned historical API producer and private retained evidence directory")
	}
	for _, kind := range []string{"tiny", "raw"} {
		t.Run(kind, func(t *testing.T) { legacyStress(t, root, kind) })
	}
}

type legacyStressManifest struct {
	Kind, Owner string
	Scoped      bool
	MaxKeySize  int
	Rows        []struct {
		ID, KeySHA256 string
		KeyBytes      int
	}
}

func legacyStress(t *testing.T, root, kind string) {
	t.Helper()
	physicalLog, err := os.OpenFile(filepath.Join(root, kind+"-physical.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer physicalLog.Close()
	// These are fresh sampled preflights, not reservations or peak bounds.
	physical := func(floor uint64) any {
		t.Helper()
		var fs unix.Statfs_t
		if err := unix.Statfs(root, &fs); err != nil {
			t.Fatal(err)
		}
		free := uint64(fs.Bavail) * uint64(fs.Bsize)
		type extent struct {
			Path      string
			Length    int64
			Allocated int64
		}
		var files []extent
		var boundary string
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Ext(path) != ".db" {
				return nil
			}
			var st unix.Stat_t
			if err := unix.Stat(path, &st); err != nil {
				return err
			}
			files = append(files, extent{path, st.Size, st.Blocks * 512})
			if st.Size >= 320<<20 {
				boundary = path
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(physicalLog).Encode(struct {
			ObservedAt  time.Time
			Clock       string
			Free, Floor uint64
			Databases   []extent
			Boundary    string
		}{time.Now(), "REAL_FILESYSTEM_SAMPLE", free, floor, files, boundary}); err != nil {
			t.Fatal(err)
		}
		if boundary != "" {
			t.Fatalf("HOLD: natural database at or above 320 MiB: %s", boundary)
		}
		if free < floor {
			t.Fatalf("HOLD: fresh free space %d below %d", free, floor)
		}
		return struct {
			Free      uint64
			Databases []extent
		}{free, files}
	}
	physical(8 << 30)
	input := filepath.Join(root, "fixtures", kind)
	var manifest legacyStressManifest
	data, err := os.ReadFile(filepath.Join(input, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	count := 10000
	if kind == "raw" {
		count = 600
	}
	if manifest.Kind != kind || !manifest.Scoped || manifest.Owner == "" || manifest.MaxKeySize != bolt.MaxKeySize || len(manifest.Rows) != count {
		t.Fatal("historical manifest shape differs")
	}
	f, err := os.Open(filepath.Join(input, "keys.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	keys := make([][]byte, count)
	seen := make(map[string]bool, count)
	var total int
	for i, row := range manifest.Rows {
		var n [4]byte
		if _, err = io.ReadFull(f, n[:]); err != nil {
			t.Fatal(err)
		}
		size := int(binary.BigEndian.Uint32(n[:]))
		if size < 1 || size > bolt.MaxKeySize || size != row.KeyBytes {
			t.Fatal("raw key bound differs")
		}
		if kind == "raw" && size != bolt.MaxKeySize {
			t.Fatal("historical maximum key not reached")
		}
		total += size
		if total > 20<<20 {
			t.Fatal("raw key corpus exceeds 20 MiB")
		}
		key := make([]byte, size)
		if _, err = io.ReadFull(f, key); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(key)
		if hex.EncodeToString(sum[:]) != row.KeySHA256 || seen[string(key)] {
			t.Fatal("key identity/hash differs")
		}
		seen[string(key)] = true
		keys[i] = key
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); n != 0 || err != io.EOF {
		t.Fatal("unbounded key manifest trailing bytes")
	}
	out := filepath.Join(root, "current", kind)
	if err = os.MkdirAll(filepath.Dir(out), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	copyFile := func(from, to string) {
		t.Helper()
		a, e := os.Open(from)
		if e != nil {
			t.Fatal(e)
		}
		defer a.Close()
		b, e := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		_, e = io.Copy(b, a)
		ce := b.Close()
		if e != nil {
			t.Fatal(e)
		}
		if ce != nil {
			t.Fatal(ce)
		}
	}
	copyFile(filepath.Join(input, "state", "jobs.db"), filepath.Join(out, "jobs.db"))
	recordFile, err := os.OpenFile(filepath.Join(out, "evidence.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer recordFile.Close()
	record := func(phase string, value any) {
		t.Helper()
		st, e := os.Stat(filepath.Join(out, "jobs.db"))
		if e != nil {
			t.Fatal(e)
		}
		if e = json.NewEncoder(recordFile).Encode(struct {
			Phase     string
			FileBytes int64
			Value     any
			Physical  any
		}{phase, st.Size(), value, physical(2 << 30)}); e != nil {
			t.Fatal(e)
		}
		if st.Size() >= 320<<20 {
			t.Fatalf("HOLD: natural %s database at or above 320 MiB at %s", kind, phase)
		}
	}
	record("old-stopped", manifest.Owner)
	physical(2 << 30)
	s, err := Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	actor := func() auth.Actor {
		t.Helper()
		e, err := auth.NewFile(filepath.Join(input, "auth.json"), s.Owner())
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(input, "admin.token"))
		if err != nil {
			t.Fatal(err)
		}
		a, err := e.Authenticate("Bearer " + string(bytes.TrimSuffix(raw, []byte("\n"))))
		if err != nil || a.Role() != auth.Admin {
			t.Fatalf("real legacy authority: %v", err)
		}
		return a
	}()
	if s.Owner() != manifest.Owner || !s.Scoped() {
		t.Fatal("migration changed owner or scoped activation")
	}
	task := backend.Task{Argv: []string{"work"}}
	for i, row := range manifest.Rows {
		got, e := s.GetFor(actor, row.ID)
		if e != nil || got.ID != row.ID || got.Status.State != backend.Canceled || got.Sequence != uint64(i+1) {
			t.Fatalf("migrated job %d: %+v %v", i, got, e)
		}
		replay, e := s.Submit(actor, string(keys[i]), task, "ignored", "ignored")
		if e != nil || replay.ID != row.ID {
			t.Fatalf("raw replay %d: %v", i, e)
		}
	}
	// Read-only inspection verifies real old global/principal mirrors independently
	// of the new request record encoding. No lower-level writes are permitted.
	if err = s.db.View(func(tx *bolt.Tx) error {
		for i, row := range manifest.Rows {
			if !bytes.Equal(tx.Bucket(requestsBucket).Get(keys[i]), []byte(row.ID)) || !bytes.Equal(tx.Bucket(principalRequestsBucket).Bucket([]byte("legacy")).Get(keys[i]), []byte(row.ID)) {
				return fmt.Errorf("legacy mirror %d differs", i)
			}
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	disabled, err := s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	record("migrated-disabled", disabled)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	physical(2 << 30)
	info, err := EnableRetention(out, DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	physical(2 << 30)
	s, err = Open(out)
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	record("enabled-defaults", current)
	usage := func(info RetentionInfo, q QuotaDomain) Usage {
		t.Helper()
		for _, u := range info.Usage {
			if u.Domain == q {
				return u
			}
		}
		t.Fatalf("quota %d absent", q)
		return Usage{}
	}
	var measuredRequestBytes uint64
	if err = s.db.View(func(tx *bolt.Tx) error {
		for _, b := range []*bolt.Bucket{tx.Bucket(requestsBucket), tx.Bucket(principalRequestsBucket).Bucket([]byte("legacy")), retentionTable(tx, retainedRequestsBucket)} {
			if e := b.ForEach(func(k, v []byte) error { measuredRequestBytes += uint64(len(k) + len(v)); return nil }); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if usage(current, RequestBytesQuota).Used != measuredRequestBytes {
		t.Fatal("raw root/principal and encoded request charges differ")
	}
	if usage(current, JobsQuota).Used != uint64(count) {
		t.Fatal("import job count differs")
	}
	if kind == "tiny" && usage(current, MetadataBytesQuota).Used < uint64(count)*16384 {
		t.Fatal("missing historical overhead charge")
	}
	if kind == "raw" && usage(current, RequestBytesQuota).Used <= DefaultRetentionPolicy().RequestBytes {
		t.Fatal("genuine historical raw-ID quota not exceeded")
	}
	hash := func() [32]byte {
		t.Helper()
		h := sha256.New()
		var walk func(*bolt.Bucket) error
		walk = func(b *bolt.Bucket) error {
			var seq [8]byte
			binary.BigEndian.PutUint64(seq[:], b.Sequence())
			h.Write(seq[:])
			return b.ForEach(func(k, v []byte) error {
				var n [8]byte
				binary.BigEndian.PutUint64(n[:], uint64(len(k)))
				h.Write(n[:])
				h.Write(k)
				if v == nil {
					h.Write([]byte{0})
					return walk(b.Bucket(k))
				}
				h.Write([]byte{1})
				binary.BigEndian.PutUint64(n[:], uint64(len(v)))
				h.Write(n[:])
				h.Write(v)
				return nil
			})
		}
		if err := s.db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, b *bolt.Bucket) error { h.Write(name); return walk(b) })
		}); err != nil {
			t.Fatal(err)
		}
		var sum [32]byte
		copy(sum[:], h.Sum(nil))
		return sum
	}
	before := hash()
	_, err = s.SubmitRetained(actor, info.Domain, "positive-growth", task, "remote", "test-binding", backend.RemoteOutput, 0)
	var capacity *CapacityError
	expected := JobsQuota
	if kind == "raw" {
		expected = RequestBytesQuota
		m := usage(current, MetadataBytesQuota)
		if m.Used+16384 > m.Limit {
			expected = MetadataBytesQuota
		}
	}
	u := usage(current, expected)
	if !errors.As(err, &capacity) || capacity.Domain != expected || capacity.Used != u.Used || capacity.Limit != u.Limit {
		t.Fatalf("governing capacity=%+v error=%v want domain%d used%d limit%d", capacity, err, expected, u.Used, u.Limit)
	}
	if hash() != before {
		t.Fatal("refused growth changed logical state/counters/sequences")
	}
	replay, err := s.Submit(actor, string(keys[0]), task, "invalid-route", "invalid-binding")
	if err != nil || replay.ID != manifest.Rows[0].ID {
		t.Fatalf("grandfathered replay: %v", err)
	}
	if hash() != before {
		t.Fatal("replay changed logical state")
	}
	// Backup/Restore are offline, preserve original state, and never run together.
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	backup := filepath.Join(out, "snapshot.db")
	physical(2 << 30)
	if err = Backup(out, backup); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(out, "restored")
	physical(2 << 30)
	if err = Restore(backup, restored); err != nil {
		t.Fatal(err)
	}
	func() {
		physical(2 << 30)
		r, err := Open(restored)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}()
		for _, row := range manifest.Rows {
			j, err := r.GetFor(actor, row.ID)
			if err != nil || j.Status.State != backend.Canceled {
				t.Fatalf("restore result %s: %v", row.ID, err)
			}
		}
		ri, err := r.RetentionInfo(actor)
		if err != nil {
			t.Fatal(err)
		}
		if r.Owner() != manifest.Owner || ri.Domain.Lineage == info.Domain.Lineage || ri.LegacyReplayOpen || usage(ri, JobsQuota).Used != uint64(count) || usage(ri, RequestBytesQuota).Used != usage(current, RequestBytesQuota).Used {
			t.Fatal("restore lost content/charges or reused replay lineage")
		}
		_, err = r.Submit(actor, string(keys[0]), task, "ignored", "ignored")
		var expired *ExpiryError
		if !errors.As(err, &expired) || expired.Reason != RequestEpochExpired {
			t.Fatalf("restored old replay: %v", err)
		}
		_, _, err = r.LookupRetained(actor, info.Domain, "modern-domain-probe", task)
		if !errors.As(err, &expired) || expired.Reason != RequestLineageChanged {
			t.Fatalf("restored old domain: %v", err)
		}
		if err = r.db.View(validateSnapshot); err != nil {
			t.Fatal(err)
		}
	}()
	physical(2 << 30)
	s, err = Open(out)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit metadata-only acknowledgement creates permanent needs_manual
	// obligations. It neither fabricates native closure nor acknowledges cleanup.
	sel := PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true, Max: uint64(count), Limit: 100}
	var marked uint32
	for pages := 0; pages < 110; pages++ {
		physical(2 << 30)
		page, e := s.Prune(actor, sel, true, nil, nil, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		marked += page.Marked
		record("metadata-prune", page)
		if !page.More {
			break
		}
		if page.After <= sel.After {
			t.Fatal("prune cursor did not advance")
		}
		sel.After = page.After
		if pages == 109 {
			t.Fatal("prune exceeded 110 page bound")
		}
	}
	if marked > 1024 || marked == 0 {
		t.Fatalf("unexpected protected cleanup marks %d", marked)
	}
	obligations, err := s.ListObligations(actor, 0, 0, 100)
	if err != nil || len(obligations.Items) == 0 {
		t.Fatalf("protected obligations: %v", err)
	}
	for _, o := range obligations.Items {
		if !o.NeedsManual {
			t.Fatal("metadata prune invented closure")
		}
	}
	// The private clock changes epoch phases only, with no concurrent control or
	// filesystem/auth clock mutation. Retirement must remain latched on rollback.
	future := info.LegacyCutoff.Add(time.Second)
	s.now = func() time.Time { return future }
	// Each closed epoch-zero request costs two subject units, and its complete
	// coupled trial fits the 512 KiB request slice. At least one retires per
	// nonempty call: the existing N+1 bound is sufficient without extension.
	previousInfo, e := s.RetentionInfo(actor)
	if e != nil {
		t.Fatal(e)
	}
	previous := usage(previousInfo, RequestRecordsQuota).Used
	if previous != uint64(count) {
		t.Fatal("unexpected sealed legacy request corpus", previous, count)
	}
	for calls := 0; calls <= count; calls++ {
		if err = s.MaintainRetention(future); err != nil {
			t.Fatal(err)
		}
		now, e := s.RetentionInfo(actor)
		if e != nil {
			t.Fatal(e)
		}
		next := usage(now, RequestRecordsQuota).Used
		if previous == 0 || next >= previous || previous-next > 25 {
			t.Fatalf("retirement progress before=%d after=%d", previous, next)
		}
		record("retirement-call", struct {
			Call                    int
			Previous, Next, Removed uint64
			Epoch                   time.Time
			Clock, MutationBytes    string
		}{calls + 1, previous, next, previous - next, future, "CLOCK_MODELED_STORE_EPOCH_ONLY", "SOURCE_ENFORCED_1_MIB_NOT_MEASURED"})
		previous = next
		if usage(now, RequestRecordsQuota).Used == 0 {
			record("retired", struct {
				Calls int
				Info  RetentionInfo
			}{calls + 1, now})
			break
		}
		if calls == count {
			t.Fatal("retirement exceeded one-record-progress corpus bound")
		}
	}
	var expired *ExpiryError
	s.now = func() time.Time { return info.LegacyCutoff.Add(-time.Hour) }
	_, err = s.Submit(actor, string(keys[0]), task, "ignored", "ignored")
	if !errors.As(err, &expired) || expired.Reason != RequestEpochExpired {
		t.Fatalf("legacy replay resurrected on rollback: %v", err)
	}
	if err = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(requestsBucket).Stats().KeyN != 0 || tx.Bucket(principalRequestsBucket).Bucket([]byte("legacy")).Stats().KeyN != 0 {
			return errors.New("retired raw mirrors remain")
		}
		if err := tx.Bucket(jobsBucket).ForEach(func(id, _ []byte) error {
			var metadata retainedJob
			if err := loadJSON(retentionTable(tx, retainedJobsBucket), id, &metadata); err != nil {
				return err
			}
			if !metadata.Legacy || !metadata.LegacyRequestRetired || metadata.LegacyRequestID != nil {
				return errors.New("closed legacy job retains raw request identity")
			}
			return nil
		}); err != nil {
			return err
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	final, err := s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	if usage(final, JobsQuota).Used != uint64(count)-uint64(marked) || usage(final, CleanupRecordsQuota).Used != uint64(marked) {
		t.Fatal("protected job/manual obligation accounting differs")
	}
	record("final-protected", final)
}
