package store

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These UNRUN source tests exercise real Store transactions and the actual
// dormant migration. File/archive receipts and root geometry are MODELED;
// no filesystem, native writer or real-elapsed-time claim follows.
func artifactTestInstall(t *testing.T, s *Store) {
	t.Helper()
	s.now = func() time.Time { return time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC) }
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return initializeArtifacts(tx, artifactRoot{Layout: 1, SchemaFloor: schemaVersion, Policy: "default-v1", ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17})
	}); err != nil {
		t.Fatal(err)
	}
}

func artifactTestUsage(t *testing.T, s *Store) artifactUsage {
	t.Helper()
	var out artifactUsage
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		_, out, _, err = st.encode()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func artifactTestComplete(t *testing.T, s *Store, actor auth.Actor) ArtifactRef {
	t.Helper()
	u, err := s.BeginArtifactUpload(actor, strings.Repeat("b", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	file := ArtifactFile{Device: 17, Inode: 42}
	u, err = s.ArtifactPartCreated(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file)
	if err != nil {
		t.Fatal(err)
	}
	u, err = s.ArtifactChunkSynced(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file, 0, 1, strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	u, err = s.BeginArtifactFinalize(actor, ArtifactCursor{u.Reference.ID, u.Revision})
	if err != nil {
		t.Fatal(err)
	}
	before := artifactTestUsage(t, s)
	if before[aPendingBytes] != 1 || before[aCompletedBytes] != 1 || before[aFiles] != 4 {
		t.Fatalf("transient reservations: %v", before)
	}
	ref, err := s.CompleteArtifactUpload(actor, ArtifactCursor{u.Reference.ID, u.Revision}, u.Reference, file, ArtifactManifest{Reference: u.Reference, ExpandedBytes: 1, RegularFiles: 1})
	if err != nil {
		t.Fatal(err)
	}
	after := artifactTestUsage(t, s)
	if after[aPendingBytes] != 0 || after[aPendingCount] != 0 || after[aCompletedBytes] != 1 || after[aFiles] != 3 {
		t.Fatalf("completed reservations: %v", after)
	}
	return ref
}

func TestArtifactUninstalledRefusesWithoutMutation(t *testing.T) {
	s := openTest(t)
	if _, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1); !errors.Is(err, ErrArtifactsDisabled) {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != schemaVersion || tx.Bucket(artifactsBucket) != nil {
			t.Fatal("dormant installation mutated")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactSchema5FenceAndDormantMigration(t *testing.T) {
	s := openTest(t)
	j, err := s.Submit(testActor(), "unchanged", backend.Task{Argv: []string{"true"}}, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	var row, request []byte
	if err := s.db.Update(func(tx *bolt.Tx) error {
		row = bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(j.ID)))
		request = bytes.Clone(tx.Bucket(requestsBucket).Get([]byte("unchanged")))
		if err := tx.DeleteBucket(workflowsBucket); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(5))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return initialize(tx, s.Owner()) }); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != schemaVersion || tx.Bucket(artifactsBucket) != nil || !bytes.Equal(row, tx.Bucket(jobsBucket).Get([]byte(j.ID))) || !bytes.Equal(request, tx.Bucket(requestsBucket).Get([]byte("unchanged"))) {
			t.Fatal("migration changed accepted bytes or invented root")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(workflowsBucket); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(5)); err != nil {
			return err
		}
		b, err := tx.CreateBucket(artifactsBucket)
		if err != nil {
			return err
		}
		return b.Put([]byte("state"), []byte("invalid JSON must not decode"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(inspect); err == nil || err.Error() != "artifact metadata requires schema 6" {
		t.Fatalf("predecode fence: %v", err)
	}
}

func TestArtifactUploadCASAndExpiry(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	c := ArtifactCursor{u.Reference.ID, u.Revision}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.ArtifactPartCreated(testActor(), c, ArtifactFile{17, 42})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrArtifactConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatalf("CAS winners %d", wins)
	}
	u, err = s.GetArtifactUpload(testActor(), c.ID)
	if err != nil {
		t.Fatal(err)
	}
	before := artifactTestUsage(t, s)
	until := u.ExpiresAt
	s.now = func() time.Time { return until }
	if _, err := s.ArtifactChunkSynced(testActor(), ArtifactCursor{c.ID, u.Revision}, ArtifactFile{17, 42}, 0, 1, strings.Repeat("c", 64)); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal(err)
	}
	if got := artifactTestUsage(t, s); got != before {
		t.Fatalf("expiry credited charge: %v -> %v", before, got)
	}
}

func TestArtifactPendingQuotaAndCancelReceipt(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	var first ArtifactUpload
	for i := 0; i < 4; i++ {
		u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = u
		}
	}
	before := artifactTestUsage(t, s)
	_, err := s.BeginArtifactUpload(testActor(), strings.Repeat("b", 64), 1)
	var cap *ArtifactCapacityError
	if !errors.As(err, &cap) || cap.Domain != "pending uploads" || cap.Used != 4 || cap.Limit != 4 {
		t.Fatalf("capacity: %v", err)
	}
	if got := artifactTestUsage(t, s); got != before {
		t.Fatal("quota failure changed counters")
	}
	id, err := s.CancelArtifactUpload(testActor(), ArtifactCursor{first.Reference.ID, first.Revision})
	if err != nil {
		t.Fatal(err)
	}
	marked := artifactTestUsage(t, s)
	if marked[aPendingBytes] != before[aPendingBytes] || marked[aPendingCount] != before[aPendingCount] {
		t.Fatal("mark credited pending")
	}
	if err := s.artifactUpdate(func(st *artifactState) error {
		d := st.Deletes[id]
		wrong := d
		wrong.Revision++
		return artifactFinishDelete(st, id, wrong)
	}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal(err)
	}
	if err := s.artifactUpdate(func(st *artifactState) error { return artifactFinishDelete(st, id, st.Deletes[id]) }); err != nil {
		t.Fatal(err)
	}
	after := artifactTestUsage(t, s)
	if after[aPendingBytes] != 3 || after[aPendingCount] != 3 {
		t.Fatalf("receipt settlement: %v", after)
	}
}

func TestArtifactOwnerAndActualReaderProtection(t *testing.T) {
	f := newScopeFixture(t)
	s := f.s
	artifactTestInstall(t, s)
	ref := artifactTestComplete(t, s, f.actors["alice"])
	if _, err := s.BeginArtifactDownload(f.actors["bob"], ref, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	lease, err := s.BeginArtifactDownload(f.actors["alice"], ref, OriginalServerLifetime{})
	if err != nil {
		t.Fatal(err)
	}
	late := s.now().Add(25 * time.Hour)
	s.now = func() time.Time { return late }
	operation := strings.Repeat("d", 32)
	if err := s.artifactUpdate(func(st *artifactState) error { return artifactMarkObjectDelete(st, ref.ID, operation, late) }); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal(err)
	}
	lease, err = s.CloseArtifactDownload(f.actors["alice"], ArtifactCursor{lease.ID, lease.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if got := artifactTestUsage(t, s); got[aDownloads] != 1 {
		t.Fatal("close intent credited lease")
	}
	if err := s.artifactUpdate(func(st *artifactState) error {
		return artifactReaderClosed(st, "alice", ArtifactCursor{lease.ID, lease.Revision}, s.now())
	}); err != nil {
		t.Fatal(err)
	}
	eligible := late.Add(time.Hour)
	s.now = func() time.Time { return eligible }
	if err := s.artifactUpdate(func(st *artifactState) error { return artifactMarkObjectDelete(st, ref.ID, operation, eligible) }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginArtifactDownload(f.actors["alice"], ref, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := s.artifactUpdate(func(st *artifactState) error { return artifactFinishDelete(st, operation, st.Deletes[operation]) }); err != nil {
		t.Fatal(err)
	}
	if got := artifactTestUsage(t, s); got[aCompletedCount] != 0 || got[aCompletedBytes] != 0 {
		t.Fatal("object charge retained after trusted unlink receipt")
	}
}

func TestArtifactAcceptPinRollback(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	ref := artifactTestComplete(t, s, testActor())
	task := backend.Task{Argv: []string{"true"}, Workdir: &artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)}}
	before := artifactTestUsage(t, s)
	bad := backend.CloneTask(task)
	bad.Workdir.SHA256 = strings.Repeat("0", 64)
	if _, err := s.SubmitWorkspace(testActor(), nil, "artifact-job", bad, "remote", "test-binding", backend.RemoteOutput, 0); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if got := artifactTestUsage(t, s); got != before {
		t.Fatal("refused acceptance leaked pin charge")
	}
	if _, found, err := s.Lookup(testActor(), "artifact-job", bad); err != nil || found {
		t.Fatalf("failed acceptance kept request: %v %v", found, err)
	}
	j, err := s.SubmitWorkspace(testActor(), nil, "artifact-job", task, "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	if j.Sequence != 1 {
		t.Fatal("failed acceptance consumed sequence")
	}
	if got := artifactTestUsage(t, s); got[aMetadataRecords] <= before[aMetadataRecords] {
		t.Fatal("committed input pin not charged")
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactCorruptionRefusesBeforeMutation(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	if _, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1); err != nil {
		t.Fatal(err)
	}
	var original []byte
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(artifactsBucket)
		original = bytes.Clone(b.Get([]byte("usage")))
		bad := bytes.Clone(original)
		bad[7] ^= 1
		return b.Put([]byte("usage"), bad)
	}); err != nil {
		t.Fatal(err)
	}
	_, err := s.BeginArtifactUpload(testActor(), strings.Repeat("b", 64), 1)
	if err == nil {
		t.Fatal("corrupt counter repaired or accepted")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		got := tx.Bucket(artifactsBucket).Get([]byte("usage"))
		if reflect.DeepEqual(got, original) {
			t.Fatal("counter silently repaired")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactWorkspaceResultExpiryRevokesCachedReference(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, true)
	ref := artifactRegressionPublish(t, s, j)
	got, err := s.ArtifactResultFor(testActor(), j.ID, j.CurrentAttemptID())
	if err != nil || got.State != "available" || got.Reference == nil || *got.Reference != ref {
		t.Fatalf("available: %+v %v", got, err)
	}
	lease, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{})
	if err != nil {
		t.Fatal(err)
	}
	before := artifactTestUsage(t, s)
	if err := s.artifactUpdate(func(st *artifactState) error {
		return artifactExpireResult(st, "legacy", j.ID, j.CurrentAttemptID(), s.now())
	}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("active read expired: %v", err)
	}
	if got := artifactTestUsage(t, s); got != before {
		t.Fatal("refused expiry changed charges")
	}
	lease, err = s.CloseArtifactDownload(testActor(), ArtifactCursor{lease.ID, lease.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.artifactUpdate(func(st *artifactState) error {
		return artifactExpireResult(st, "legacy", j.ID, j.CurrentAttemptID(), s.now())
	}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("closing read expired before actual FD close: %v", err)
	}
	closure := ArtifactDownloadClosure{Owner: "legacy", LeaseID: lease.ID, Revision: lease.Revision, Ref: ref, File: lease.File, RootID: lease.RootID}
	if err := s.FinishArtifactDownload(closure, s.now()); err != nil {
		t.Fatal(err)
	}
	page, err := s.Prune(testActor(), PruneSelection{Before: s.now().Add(time.Hour), Mode: ResultOnly, Attempt: 1}, true, nil, nil, s.now())
	if err != nil || page.Marked != 1 {
		t.Fatalf("result prune: %+v %v", page, err)
	}
	got, err = s.ArtifactResultFor(testActor(), j.ID, j.CurrentAttemptID())
	if err != nil || got.State != "expired" || got.Reference != nil || got.AttemptID != j.CurrentAttemptID() {
		t.Fatalf("expired: %+v %v", got, err)
	}
	if _, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cached Ref survived revocation: %v", err)
	}
	if usage := artifactTestUsage(t, s); usage[aDownloads] != 0 || usage[aCompletedBytes] != 2 {
		t.Fatalf("expiry credited object: %v", usage)
	}
}
