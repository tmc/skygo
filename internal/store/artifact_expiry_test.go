package store

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// These tests exercise actual public Store transactions with MODELED clock,
// file and root receipts. They establish no elapsed-hour or native unlink proof.
func artifactExpiryUpload(t *testing.T, s *Store, state string, inode uint64) ArtifactUpload {
	t.Helper()
	u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("b", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	if state == "creating" {
		return u
	}
	file := ArtifactFile{Device: 17, Inode: inode}
	u, err = s.ArtifactPartCreated(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, file)
	if err != nil {
		t.Fatal(err)
	}
	if state == "writable" {
		return u
	}
	u, err = s.ArtifactChunkSynced(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, file, 0, 1, strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	u, err = s.BeginArtifactFinalize(testActor(), ArtifactCursor{u.Reference.ID, u.Revision})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func artifactExpiryAt(s *Store, now time.Time) {
	s.now = func() time.Time { return now }
}

func artifactExpiryObserve(t *testing.T, s *Store, u ArtifactUpload, file ArtifactFile) ArtifactUploadExpiry {
	t.Helper()
	installation, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	return ArtifactUploadExpiry{Owner: testActor().Principal(), Upload: u, File: file, RootID: installation.ID}
}

func TestArtifactUploadExpirySelection(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	complete := artifactTestComplete(t, s, testActor())
	creating := artifactExpiryUpload(t, s, "creating", 0)
	writable := artifactExpiryUpload(t, s, "writable", 51)
	finalizing := artifactExpiryUpload(t, s, "finalizing", 52)
	deleting := artifactExpiryUpload(t, s, "writable", 53)
	if _, err := s.CancelArtifactUpload(testActor(), ArtifactCursor{deleting.Reference.ID, deleting.Revision}); err != nil {
		t.Fatal(err)
	}
	before := artifactDatabaseBytes(t, s)
	artifactExpiryAt(s, creating.ExpiresAt.Add(-time.Nanosecond))
	if got, found, err := s.NextArtifactUploadExpiry(""); err != nil || found || got != (ArtifactUploadExpiry{}) {
		t.Fatalf("future upload selected: %+v %t %v", got, found, err)
	}
	artifactExpiryAt(s, creating.ExpiresAt)
	want := map[string]ArtifactUploadExpiry{
		creating.Reference.ID:   artifactExpiryObserve(t, s, creating, ArtifactFile{}),
		writable.Reference.ID:   artifactExpiryObserve(t, s, writable, ArtifactFile{17, 51}),
		finalizing.Reference.ID: artifactExpiryObserve(t, s, finalizing, ArtifactFile{17, 52}),
	}
	ids := []string{creating.Reference.ID, writable.Reference.ID, finalizing.Reference.ID}
	sort.Strings(ids)
	cursor := ""
	for i := 0; i < 2*len(ids); i++ {
		got, found, err := s.NextArtifactUploadExpiry(cursor)
		id := ids[i%len(ids)]
		if err != nil || !found || got != want[id] {
			t.Fatalf("selection %d: %+v %t %v, want %+v", i, got, found, err, want[id])
		}
		if got.Upload.Reference.ID == complete.ID || got.Upload.Reference.ID == deleting.Reference.ID {
			t.Fatal("completed or deleting upload selected")
		}
		cursor = id
	}
	if got, found, err := s.NextArtifactUploadExpiry(strings.Repeat("f", 32)); err != nil || !found || got != want[ids[0]] {
		t.Fatalf("high cursor did not wrap: %+v %t %v", got, found, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("selection changed durable metadata")
	}
}

func TestArtifactUploadExpiryMark(t *testing.T) {
	for _, retained := range []bool{false, true} {
		for _, state := range []string{"writable", "finalizing"} {
			name := map[bool]string{false: "retention-off", true: "retention-on"}[retained] + "/" + state
			t.Run(name, func(t *testing.T) {
				var s *Store
				if retained {
					policy := DefaultRetentionPolicy()
					policy.LocalOutputEnabled = false
					s, _ = enableTest(t, policy)
				} else {
					s = openTest(t)
				}
				artifactTestInstall(t, s)
				u := artifactExpiryUpload(t, s, state, 71)
				artifactExpiryAt(s, u.ExpiresAt)
				expected, found, err := s.NextArtifactUploadExpiry("")
				if err != nil || !found {
					t.Fatalf("select: %t %v", found, err)
				}
				before := artifactTestUsage(t, s)
				work, err := s.ExpireArtifactUpload(expected)
				kind := "upload"
				if state == "finalizing" {
					kind = "finalizing"
				}
				if err != nil || work.Kind != kind || work.ID == "" || work.Owner != expected.Owner || work.Ref != u.Reference || work.File != expected.File || work.RootID != expected.RootID || work.Revision != u.Revision+1 || work.Digest == ([32]byte{}) {
					t.Fatalf("mark: %+v %v", work, err)
				}
				selected, found, err := s.NextArtifactCleanup("")
				if err != nil || !found || selected.Delete == nil || *selected.Delete != work || selected.Workspace != nil {
					t.Fatalf("not actual committed work: %+v %t %v", selected, found, err)
				}
				after := artifactTestUsage(t, s)
				for _, domain := range []int{aPendingBytes, aPendingCount, aCompletedBytes, aCompletedCount, aMetadataBytes, aMetadataRecords, aDownloads, aWorkspaces, aAllocatedBytes, aFiles} {
					if after[domain] != before[domain] {
						t.Fatalf("mark credited domain %d: %d -> %d", domain, before[domain], after[domain])
					}
				}
				if _, found, err := s.NextArtifactUploadExpiry(""); err != nil || found {
					t.Fatalf("marked upload reselected: %t %v", found, err)
				}
				unchanged := artifactDatabaseBytes(t, s)
				if got, err := s.ExpireArtifactUpload(expected); !errors.Is(err, ErrArtifactConflict) || got != (ArtifactDeleteWork{}) {
					t.Fatalf("repeated stale mark: %+v %v", got, err)
				}
				if !bytes.Equal(unchanged, artifactDatabaseBytes(t, s)) {
					t.Fatal("stale mark changed database")
				}
				if err := s.db.View(validateSnapshot); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestArtifactUploadExpiryCompareEveryField(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u := artifactExpiryUpload(t, s, "writable", 72)
	artifactExpiryAt(s, u.ExpiresAt)
	expected, found, err := s.NextArtifactUploadExpiry("")
	if err != nil || !found {
		t.Fatalf("select: %t %v", found, err)
	}
	cases := []struct {
		name   string
		change func(*ArtifactUploadExpiry)
	}{
		{"owner", func(e *ArtifactUploadExpiry) { e.Owner = "alice" }},
		{"root", func(e *ArtifactUploadExpiry) { e.RootID = strings.Repeat("d", 32) }},
		{"id", func(e *ArtifactUploadExpiry) { e.Upload.Reference.ID = strings.Repeat("d", 32) }},
		{"digest", func(e *ArtifactUploadExpiry) { e.Upload.Reference.SHA256 = strings.Repeat("d", 64) }},
		{"size", func(e *ArtifactUploadExpiry) { e.Upload.Reference.Size++ }},
		{"revision", func(e *ArtifactUploadExpiry) { e.Upload.Revision++ }},
		{"state", func(e *ArtifactUploadExpiry) { e.Upload.State = "finalizing" }},
		{"ack", func(e *ArtifactUploadExpiry) { e.Upload.Acknowledged++ }},
		{"expiry", func(e *ArtifactUploadExpiry) { e.Upload.ExpiresAt = e.Upload.ExpiresAt.Add(time.Nanosecond) }},
		{"device", func(e *ArtifactUploadExpiry) { e.File.Device++ }},
		{"inode", func(e *ArtifactUploadExpiry) { e.File.Inode++ }},
		{"unknown", func(e *ArtifactUploadExpiry) { e.File = ArtifactFile{} }},
	}
	before := artifactDatabaseBytes(t, s)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := expected
			tc.change(&changed)
			got, err := s.ExpireArtifactUpload(changed)
			if !errors.Is(err, ErrArtifactConflict) || got != (ArtifactDeleteWork{}) {
				t.Fatalf("mismatch accepted: %+v %v", got, err)
			}
			if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
				t.Fatal("mismatch changed database")
			}
		})
	}
	artifactExpiryAt(s, u.ExpiresAt.Add(-time.Nanosecond))
	if _, err := s.ExpireArtifactUpload(expected); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("clock rollback bypassed fixed deadline: %v", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("future mark changed database")
	}
}

func TestArtifactUploadExpiryUnknownAndAdmissionCap(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	for i := 0; i < 4; i++ {
		artifactExpiryUpload(t, s, "creating", 0)
	}
	before := artifactDatabaseBytes(t, s)
	if _, err := s.BeginArtifactUpload(testActor(), strings.Repeat("c", 64), 1); err == nil {
		t.Fatal("fifth principal upload admitted")
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("capacity refusal changed database")
	}
	artifactExpiryAt(s, s.now().Add(time.Hour))
	expected, found, err := s.NextArtifactUploadExpiry("")
	if err != nil || !found || expected.Upload.State != "creating" || expected.File != (ArtifactFile{}) {
		t.Fatalf("unknown not explicitly held: %+v %t %v", expected, found, err)
	}
	if got, err := s.ExpireArtifactUpload(expected); !errors.Is(err, ErrArtifactConflict) || got != (ArtifactDeleteWork{}) {
		t.Fatalf("unknown marked: %+v %v", got, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("unknown expiry changed metadata or credited capacity")
	}
	if _, found, err := s.NextArtifactCleanup(""); err != nil || found {
		t.Fatalf("unknown cleanup dispatched: %t %v", found, err)
	}
}

func TestArtifactUploadExpiryStaleAfterChunk(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u := artifactExpiryUpload(t, s, "writable", 73)
	expected := artifactExpiryObserve(t, s, u, ArtifactFile{17, 73})
	u, err := s.ArtifactChunkSynced(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, expected.File, 0, 1, strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	artifactExpiryAt(s, u.ExpiresAt)
	before := artifactDatabaseBytes(t, s)
	if got, err := s.ExpireArtifactUpload(expected); !errors.Is(err, ErrArtifactConflict) || got != (ArtifactDeleteWork{}) {
		t.Fatalf("old ack/revision marked: %+v %v", got, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("stale chunk cursor changed database")
	}
}

func TestArtifactUploadExpiryCancelRace(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u := artifactExpiryUpload(t, s, "writable", 74)
	expected := artifactExpiryObserve(t, s, u, ArtifactFile{17, 74})
	artifactExpiryAt(s, u.ExpiresAt)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var mark ArtifactDeleteWork
	var markErr, cancelErr error
	var cancelID string
	go func() { defer wg.Done(); <-start; mark, markErr = s.ExpireArtifactUpload(expected) }()
	go func() {
		defer wg.Done()
		<-start
		cancelID, cancelErr = s.CancelArtifactUpload(testActor(), ArtifactCursor{u.Reference.ID, u.Revision})
	}()
	close(start)
	wg.Wait()
	if (markErr == nil) == (cancelErr == nil) {
		t.Fatalf("expected one serialized winner: expiry=%v cancel=%v", markErr, cancelErr)
	}
	wantID := cancelID
	if markErr == nil {
		wantID = mark.ID
		if !errors.Is(cancelErr, ErrArtifactConflict) {
			t.Fatal(cancelErr)
		}
	} else if !errors.Is(markErr, ErrArtifactConflict) || mark != (ArtifactDeleteWork{}) {
		t.Fatalf("expiry loser exported work: %+v %v", mark, markErr)
	}
	got, found, err := s.NextArtifactCleanup("")
	if err != nil || !found || got.Delete == nil || got.Delete.ID != wantID || got.Delete.Revision != u.Revision+1 {
		t.Fatalf("winner not durable: %+v %t %v", got, found, err)
	}
	if usage := artifactTestUsage(t, s); usage[aPendingCount] != 1 || usage[aPendingBytes] != 1 {
		t.Fatalf("race credited pending reservations: %v", usage)
	}
}

func TestArtifactUploadExpiryExhaustedRevision(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u := artifactExpiryUpload(t, s, "writable", 75)
	// MODELED persisted boundary; no production seam or clock policy change.
	if err := s.artifactUpdate(func(st *artifactState) error {
		x := st.Uploads[u.Reference.ID]
		x.Revision = math.MaxUint64
		st.Uploads[u.Reference.ID] = x
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	artifactExpiryAt(s, u.ExpiresAt)
	expected, found, err := s.NextArtifactUploadExpiry("")
	if err != nil || !found {
		t.Fatalf("select: %t %v", found, err)
	}
	before := artifactDatabaseBytes(t, s)
	if got, err := s.ExpireArtifactUpload(expected); !errors.Is(err, ErrArtifactConflict) || got != (ArtifactDeleteWork{}) {
		t.Fatalf("exhausted mark: %+v %v", got, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("exhaustion changed database")
	}
}

func TestArtifactUploadExpiryDisabledAndCorrupt(t *testing.T) {
	s := openTest(t)
	if _, found, err := s.NextArtifactUploadExpiry(""); found || !errors.Is(err, ErrArtifactsDisabled) {
		t.Fatalf("disabled select: %t %v", found, err)
	}
	if got, err := s.ExpireArtifactUpload(ArtifactUploadExpiry{}); !errors.Is(err, ErrArtifactsDisabled) || got != (ArtifactDeleteWork{}) {
		t.Fatalf("disabled mark: %+v %v", got, err)
	}
	artifactTestInstall(t, s)
	if _, found, err := s.NextArtifactUploadExpiry("bad"); found || !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("invalid cursor: %t %v", found, err)
	}
	if got, found, err := s.NextArtifactUploadExpiry(""); err != nil || found || got != (ArtifactUploadExpiry{}) {
		t.Fatalf("empty select: %+v %t %v", got, found, err)
	}
	u := artifactExpiryUpload(t, s, "writable", 76)
	expected := artifactExpiryObserve(t, s, u, ArtifactFile{17, 76})
	artifactExpiryAt(s, u.ExpiresAt)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(artifactsBucket).Bucket([]byte("uploads")).Put([]byte(u.Reference.ID), []byte("{}"))
	}); err != nil {
		t.Fatal(err)
	}
	before := artifactDatabaseBytes(t, s)
	if got, found, err := s.NextArtifactUploadExpiry(""); err == nil || found || got != (ArtifactUploadExpiry{}) {
		t.Fatalf("corrupt select accepted: %+v %t %v", got, found, err)
	}
	if got, err := s.ExpireArtifactUpload(expected); err == nil || got != (ArtifactDeleteWork{}) {
		t.Fatalf("corrupt mark accepted: %+v %v", got, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("corrupt metadata was rewritten")
	}
}

func TestArtifactUploadExpiryPreservesCancelUnknown(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u := artifactExpiryUpload(t, s, "creating", 0)
	before := artifactTestUsage(t, s)
	id, err := s.CancelArtifactUpload(testActor(), ArtifactCursor{u.Reference.ID, u.Revision})
	if err != nil || id == "" {
		t.Fatalf("authenticated unknown cancel changed: %q %v", id, err)
	}
	got, err := s.GetArtifactUpload(testActor(), u.Reference.ID)
	if err != nil || got.State != "deleting" || got.Revision != u.Revision+1 {
		t.Fatalf("unknown cancel not recorded: %+v %v", got, err)
	}
	if after := artifactTestUsage(t, s); after != before {
		t.Fatalf("unknown cancel changed reservations: %v -> %v", before, after)
	}
	if _, found, err := s.NextArtifactCleanup(""); err != nil || found {
		t.Fatalf("unknown cancel invented physical work: %t %v", found, err)
	}
}

func TestArtifactUploadExpiryCompleteAndFinalizeRace(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u := artifactExpiryUpload(t, s, "finalizing", 77)
	expected := artifactExpiryObserve(t, s, u, ArtifactFile{17, 77})
	artifactExpiryAt(s, u.ExpiresAt)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var work ArtifactDeleteWork
	var expiryErr, completeErr error
	go func() { defer wg.Done(); <-start; work, expiryErr = s.ExpireArtifactUpload(expected) }()
	go func() {
		defer wg.Done()
		<-start
		_, completeErr = s.CompleteArtifactUpload(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, u.Reference, expected.File, ArtifactManifest{Reference: u.Reference, ExpandedBytes: 1, RegularFiles: 1})
	}()
	close(start)
	wg.Wait()
	if expiryErr != nil || work.Kind != "finalizing" || !errors.Is(completeErr, ErrArtifactConflict) {
		t.Fatalf("publication after deadline: expiry=%+v %v complete=%v", work, expiryErr, completeErr)
	}
	if _, err := s.BeginArtifactFinalize(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("stale finalization revived marked upload: %v", err)
	}
	if usage := artifactTestUsage(t, s); usage[aPendingBytes] != 1 || usage[aCompletedBytes] != 1 || usage[aPendingCount] != 1 || usage[aCompletedCount] != 1 {
		t.Fatalf("lost finalizing reservations: %v", usage)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if len(st.Grants) != 0 || st.Objects[u.Reference.ID].State != "publishing" {
			t.Fatal("expiry manufactured publication or discarded both-name intent")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactUploadExpiryOldObservationAfterCompletion(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u := artifactExpiryUpload(t, s, "finalizing", 78)
	expected := artifactExpiryObserve(t, s, u, ArtifactFile{17, 78})
	if _, err := s.CompleteArtifactUpload(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, u.Reference, expected.File, ArtifactManifest{Reference: u.Reference, ExpandedBytes: 1, RegularFiles: 1}); err != nil {
		t.Fatal(err)
	}
	artifactExpiryAt(s, u.ExpiresAt)
	before := artifactDatabaseBytes(t, s)
	if got, err := s.ExpireArtifactUpload(expected); !errors.Is(err, ErrArtifactConflict) || got != (ArtifactDeleteWork{}) {
		t.Fatalf("completed upload marked: %+v %v", got, err)
	}
	if _, found, err := s.NextArtifactUploadExpiry(""); err != nil || found {
		t.Fatalf("completed upload selected: %t %v", found, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("completed refusal changed grant or metadata")
	}
}
