package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Real Store transactions; the clock and artifact root geometry/receipts here
// are MODELED. The separate genuine13 gate uses actual Files and old writer.
func artifactBeginTest(t *testing.T) (*Store, RetentionInfo) {
	t.Helper()
	p := DefaultRetentionPolicy()
	p.LocalOutputEnabled = false
	s, info := enableTest(t, p)
	artifactTestInstall(t, s)
	now := info.AcceptUntil.Add(-p.Window).Add(time.Second)
	s.now = func() time.Time { return now }
	return s, info
}

func TestArtifactBeginRequestReplay(t *testing.T) {
	s, info := artifactBeginTest(t)
	sha := strings.Repeat("b", 64)
	// Introduce the same owner through the existing legacy path first, so
	// keyed admission adds two rows and the existing prepaid cleanup intent.
	if _, err := s.BeginArtifactUpload(testActor(), sha, 1); err != nil {
		t.Fatal(err)
	}
	before := artifactTestUsage(t, s)
	u, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "one", sha, 1)
	if err != nil || !created || u.State != "creating" {
		t.Fatalf("first=%+v created=%t err=%v", u, created, err)
	}
	after := artifactTestUsage(t, s)
	if after[aMetadataRecords] != before[aMetadataRecords]+3 || after[aMetadataBytes] != before[aMetadataBytes]+2*(2048+32)+1024+32 {
		t.Fatalf("keyed rows uncharged: before=%v after=%v", before, after)
	}
	disk, err := os.ReadFile(s.db.Path())
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		got, found, err := s.LookupArtifactUploadRequest(testActor(), info.Domain, "one", sha, 1)
		if err != nil || !found || got != u {
			t.Fatalf("lookup=%+v found=%t err=%v", got, found, err)
		}
		got, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "one", sha, 1)
		if err != nil || created || got != u {
			t.Fatalf("replay=%+v created=%t err=%v", got, created, err)
		}
	}
	for _, mismatch := range []struct {
		sha  string
		size uint64
	}{{strings.Repeat("c", 64), 1}, {sha, 2}} {
		got, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "one", mismatch.sha, mismatch.size)
		if !errors.Is(err, ErrArtifactConflict) || created || got != (ArtifactUpload{}) {
			t.Fatalf("mismatch=%+v created=%t err=%v", got, created, err)
		}
	}
	current, err := os.ReadFile(s.db.Path())
	if err != nil || !bytes.Equal(disk, current) || artifactTestUsage(t, s) != after {
		t.Fatalf("replay/refusal changed metadata or quota: %v", err)
	}
}

func TestArtifactBeginRequestConcurrent(t *testing.T) {
	s, info := artifactBeginTest(t)
	type result struct {
		u       ArtifactUpload
		created bool
		err     error
	}
	results := make(chan result, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "concurrent", strings.Repeat("b", 64), 1)
			results <- result{u, created, err}
		}()
	}
	wg.Wait()
	close(results)
	var first ArtifactUpload
	wins := 0
	for r := range results {
		if r.err != nil {
			t.Fatal(r.err)
		}
		if first == (ArtifactUpload{}) {
			first = r.u
		}
		if r.u != first {
			t.Fatalf("multiple reservations: %+v and %+v", first, r.u)
		}
		if r.created {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("%d create permissions, want 1", wins)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if len(st.Uploads) != 1 || len(st.BeginRequests) != 1 {
			t.Fatalf("rows=%d/%d", len(st.Uploads), len(st.BeginRequests))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactBeginRequestDomainAndExpiry(t *testing.T) {
	s, info := artifactBeginTest(t)
	sha := strings.Repeat("b", 64)
	u, _, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "existing", sha, 1)
	if err != nil {
		t.Fatal(err)
	}
	// MODELED complete metadata is not needed: while the upload is creating,
	// its fixed hour expiry refuses replay without granting a new creator.
	s.now = func() time.Time { return u.ExpiresAt }
	if got, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "existing", sha, 1); !errors.Is(err, ErrArtifactExpired) || created || got != (ArtifactUpload{}) {
		t.Fatalf("expired replay=%+v %t %v", got, created, err)
	}
	if n, err := s.RetireArtifactUploadRequests(); err != nil || n != 0 {
		t.Fatalf("early retirement=%d %v", n, err)
	}
	// Domain closure also refuses a key never accepted; an existing witness
	// is retained until retry retirement, independently of upload cleanup.
	s.now = func() time.Time { return info.AcceptUntil }
	if _, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "new", sha, 1); created || err == nil {
		t.Fatalf("closed domain admitted: %t %v", created, err)
	}
	s.now = func() time.Time { return info.RetryUntil }
	usage := artifactTestUsage(t, s)
	if n, err := s.RetireArtifactUploadRequests(); err != nil || n != 1 {
		t.Fatalf("retirement=%d %v", n, err)
	}
	after := artifactTestUsage(t, s)
	if after[aPendingBytes] != usage[aPendingBytes] || after[aFiles] != usage[aFiles] || after[aMetadataRecords]+1 != usage[aMetadataRecords] {
		t.Fatalf("retirement credited physical ownership: %v -> %v", usage, after)
	}
	if _, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "existing", sha, 1); created || err == nil {
		t.Fatalf("retired request resurrected: %t %v", created, err)
	}
}

func TestArtifactBeginRequestDeletedUploadTombstone(t *testing.T) {
	s, info := artifactBeginTest(t)
	sha := strings.Repeat("b", 64)
	u, _, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "gone", sha, 1)
	if err != nil {
		t.Fatal(err)
	}
	// MODEL the post-cleanup state, not a physical unlink receipt. The strict
	// new family explicitly permits a witness after its upload has gone.
	if err := s.artifactUpdate(func(st *artifactState) error { delete(st.Uploads, u.Reference.ID); return nil }); err != nil {
		t.Fatal(err)
	}
	before := artifactTestUsage(t, s)
	if got, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "gone", sha, 1); !errors.Is(err, ErrArtifactExpired) || created || got != (ArtifactUpload{}) {
		t.Fatalf("lost tombstone: %+v %t %v", got, created, err)
	}
	if got := artifactTestUsage(t, s); got != before {
		t.Fatal("expired memo created another reservation")
	}
}

func TestArtifactBeginRequestRetirementBound(t *testing.T) {
	s, info := artifactBeginTest(t)
	u, _, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "held", strings.Repeat("b", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	// MODEL nineteen already-collected uploads' retained witnesses. This
	// avoids inventing physical cleanup receipts or bypassing pending quota.
	if err := s.artifactUpdate(func(st *artifactState) error {
		for i := 1; i < 20; i++ {
			r := st.BeginRequests[u.Reference.ID]
			r.Ref.ID = fmt.Sprintf("%032x", i)
			r.RequestID = fmt.Sprintf("old-%d", i)
			st.BeginRequests[r.Ref.ID] = r
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return info.RetryUntil }
	before := artifactTestUsage(t, s)
	for _, want := range []int{16, 4, 0} {
		if n, err := s.RetireArtifactUploadRequests(); err != nil || n != want {
			t.Fatalf("retire=%d %v, want%d", n, err, want)
		}
	}
	after := artifactTestUsage(t, s)
	if after[aPendingBytes] != before[aPendingBytes] || after[aPendingCount] != before[aPendingCount] || after[aFiles] != before[aFiles] {
		t.Fatalf("memo retirement released upload: %v -> %v", before, after)
	}
}

func TestArtifactBeginRequestMalformedDoesNotWrite(t *testing.T) {
	s, info := artifactBeginTest(t)
	before, err := os.ReadFile(s.db.Path())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		d       RequestDomain
		id, sha string
		size    uint64
	}{
		{RequestDomain{}, "key", strings.Repeat("b", 64), 1},
		{info.Domain, "bad key", strings.Repeat("b", 64), 1},
		{info.Domain, "key", "notdigest", 1},
		{info.Domain, "key", strings.Repeat("b", 64), 0},
		{RequestDomain{strings.Repeat("f", 32), 1}, "key", strings.Repeat("b", 64), 1},
		{RequestDomain{info.Domain.Lineage, info.Domain.Epoch + 1}, "key", strings.Repeat("b", 64), 1},
	} {
		if got, created, err := s.BeginArtifactUploadRequest(testActor(), tc.d, tc.id, tc.sha, tc.size); err == nil || created || got != (ArtifactUpload{}) {
			t.Fatalf("invalid admission: %+v %t %v", got, created, err)
		}
	}
	after, err := os.ReadFile(s.db.Path())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refusal wrote metadata: %v", err)
	}
}

func TestArtifactBeginRequestClockRollback(t *testing.T) {
	for _, boundary := range []string{"accept", "retry"} {
		t.Run(boundary, func(t *testing.T) {
			s, info := artifactBeginTest(t)
			sha := strings.Repeat("b", 64)
			when := info.AcceptUntil
			key := "new"
			if boundary == "retry" {
				when = info.RetryUntil
				key = "existing"
				if _, _, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, key, sha, 1); err != nil {
					t.Fatal(err)
				}
			}
			original := s.now()
			s.now = func() time.Time { return when }
			if _, _, err := s.LookupArtifactUploadRequest(testActor(), info.Domain, key, sha, 1); err == nil {
				t.Fatal("boundary lookup accepted")
			}
			// MODELED clock rollback cannot reopen the domain after the
			// preceding real transaction durably observed its cutoff.
			s.now = func() time.Time { return original }
			if got, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, key, sha, 1); err == nil || created || got != (ArtifactUpload{}) {
				t.Fatalf("clock rollback reopened: %+v %t %v", got, created, err)
			}
			var st retentionState
			if err := s.db.View(func(tx *bolt.Tx) error { var err error; st, err = loadRetention(tx); return err }); err != nil {
				t.Fatal(err)
			}
			if boundary == "retry" && st.RetiredThrough < info.Domain.Epoch {
				t.Fatal("retry cutoff was not persisted")
			}
			if boundary == "accept" && (!st.Epochs[0].AcceptClosed) {
				t.Fatal("accept cutoff was not persisted")
			}
		})
	}
}

func TestArtifactBeginRequestQuotaRollback(t *testing.T) {
	s, info := artifactBeginTest(t)
	sha := strings.Repeat("b", 64)
	for i := 0; i < 4; i++ {
		if _, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, fmt.Sprintf("key-%d", i), sha, 1); err != nil || !created {
			t.Fatalf("admit%d: %t %v", i, created, err)
		}
	}
	before := artifactTestUsage(t, s)
	if got, created, err := s.BeginArtifactUploadRequest(testActor(), info.Domain, "full", sha, 1); err == nil || created || got != (ArtifactUpload{}) {
		t.Fatalf("quota failure exposed provisional permission: %+v %t %v", got, created, err)
	}
	if got, found, err := s.LookupArtifactUploadRequest(testActor(), info.Domain, "full", sha, 1); err != nil || found || got != (ArtifactUpload{}) {
		t.Fatalf("failed admission persisted witness: %+v %t %v", got, found, err)
	}
	if after := artifactTestUsage(t, s); after != before {
		t.Fatal("failed quota admission changed charges")
	}
}
