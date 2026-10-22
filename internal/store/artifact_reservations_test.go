package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"

	bolt "go.etcd.io/bbolt"
)

// UNRUN source tests use actual metadata transactions with MODELED archive,
// root, file and writer/removal receipts. They qualify no real physical sample.
func reservationSnapshot(t *testing.T, s *Store) ArtifactReservations {
	t.Helper()
	before := artifactDatabaseBytes(t, s)
	out, err := s.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("reservation View mutated database")
	}
	if out.Installation.Enabled {
		copy := out
		copy.Digest = [32]byte{}
		raw, err := json.Marshal(copy)
		if err != nil {
			t.Fatal(err)
		}
		if out.Digest != sha256.Sum256(append([]byte("skygo-artifact-reservations-v1\x00"), raw...)) {
			t.Fatal("digest does not describe exact returned canonical projection")
		}
		for i := 1; i < len(out.Subjects); i++ {
			a, b := out.Subjects[i-1], out.Subjects[i]
			if a.Kind > b.Kind || a.Kind == b.Kind && a.ID >= b.ID {
				t.Fatal("noncanonical or duplicate cohort")
			}
		}
	}
	return out
}

func reservationSubject(t *testing.T, s ArtifactReservations, kind, id string) ArtifactPhysicalSubject {
	t.Helper()
	for _, p := range s.Subjects {
		if p.Kind == kind && p.ID == id {
			return p
		}
	}
	t.Fatalf("missing %s/%s subject", kind, id)
	return ArtifactPhysicalSubject{}
}

func TestArtifactReservationsDormantAndFixedRootAllowance(t *testing.T) {
	s := openTest(t)
	disabled := reservationSnapshot(t, s)
	if disabled.Installation.Enabled || disabled.Digest != ([32]byte{}) || disabled.Subjects != nil || disabled.AllocatedBytes != 0 || disabled.Files != 0 {
		t.Fatalf("dormant fields must be unavailable: %+v", disabled)
	}
	artifactTestInstall(t, s)
	root := reservationSnapshot(t, s)
	if !root.Installation.Enabled || root.Installation.ID != strings.Repeat("a", 32) || root.Installation.Unit != 4096 || root.Installation.Volume != 17 || root.AllocatedBytes != 2*4096 || root.Files != 2 || len(root.Subjects) != 0 || root.Digest == ([32]byte{}) {
		t.Fatalf("fixed root allowance: %+v", root)
	}
	// Enabled=false means no logical allowance is available; neither snapshot
	// is a measured zero-block filesystem observation or root adoption proof.
}

func TestArtifactReservationsAckNoiseAndStructuralTransitions(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	unknown := reservationSnapshot(t, s)
	part := reservationSubject(t, unknown, "upload", u.Reference.ID)
	if part.File != (ArtifactFile{}) || unknown.AllocatedBytes == 0 || unknown.Files != 3 {
		t.Fatal("unknown creating File must retain nonzero allowance")
	}
	file := ArtifactFile{17, 100}
	u, err = s.ArtifactPartCreated(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, file)
	if err != nil {
		t.Fatal(err)
	}
	bound := reservationSnapshot(t, s)
	if bound.Digest == unknown.Digest || bound.AllocatedBytes != unknown.AllocatedBytes || bound.Files != unknown.Files {
		t.Fatal("new exact part identity did not invalidate same allowance")
	}
	for _, offset := range []uint64{0, 64 << 10} {
		u, err = s.ArtifactChunkSynced(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, file, offset, 64<<10, strings.Repeat("b", 64))
		if err != nil {
			t.Fatal(err)
		}
		acked := reservationSnapshot(t, s)
		if !reflect.DeepEqual(acked, bound) {
			t.Fatal("Ack/revision-only metadata invalidated prepaid physical cohort")
		}
	}
	u, err = s.BeginArtifactFinalize(testActor(), ArtifactCursor{u.Reference.ID, u.Revision})
	if err != nil {
		t.Fatal(err)
	}
	publishing := reservationSnapshot(t, s)
	// Pending+publishing each reserve their own allocation until actual seal
	// settlement. These are logical allowances, not two observed physical files.
	allocation := uint64(128<<10) + 2*4096
	if publishing.Digest == bound.Digest || publishing.AllocatedBytes != 2*4096+2*allocation || publishing.Files != 4 || len(publishing.Subjects) != 2 {
		t.Fatalf("new publishing reservation: %+v", publishing)
	}
	if reservationSubject(t, publishing, "object", u.Reference.ID).File != (ArtifactFile{}) {
		t.Fatal("publishing object adopted upload Part as sealed identity")
	}
	u, err = s.BeginArtifactFinalize(testActor(), ArtifactCursor{u.Reference.ID, u.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if got := reservationSnapshot(t, s); !reflect.DeepEqual(got, publishing) {
		t.Fatal("finalizing replay changed structural projection")
	}
	ref, err := s.CompleteArtifactUpload(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, u.Reference, file, ArtifactManifest{Reference: u.Reference, ExpandedBytes: 1, RegularFiles: 1})
	if err != nil {
		t.Fatal(err)
	}
	complete := reservationSnapshot(t, s)
	if complete.Digest == publishing.Digest || complete.AllocatedBytes != 2*4096+allocation || complete.Files != 3 || len(complete.Subjects) != 2 {
		t.Fatalf("settlement allowance double counted subject DTOs: %+v", complete)
	}
	if reservationSubject(t, complete, "object", ref.ID).File != file || reservationSubject(t, complete, "upload", ref.ID).File != file {
		t.Fatal("distinct metadata subjects lost same actual inode identity")
	}
	lease, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{})
	if err != nil {
		t.Fatal(err)
	}
	if got := reservationSnapshot(t, s); !reflect.DeepEqual(got, complete) {
		t.Fatal("read lease added a nonexistent physical subject")
	}
	closing := ArtifactDownloadClosure{Owner: "legacy", LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID}
	closing, err = s.MarkArtifactDownloadClosing(closing)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FinishArtifactDownload(closing, s.now()); err != nil {
		t.Fatal(err)
	}
	if got := reservationSnapshot(t, s); !reflect.DeepEqual(got, complete) {
		t.Fatal("lease close/revision changed physical cohort")
	}
	now := s.now().Add(25 * time.Hour)
	s.now = func() time.Time { return now }
	if _, err := s.RemoveArtifact(testActor(), ref); err != nil {
		t.Fatal(err)
	}
	work, found, err := s.NextArtifactCleanup("")
	if err != nil || !found || work.Delete == nil {
		t.Fatal("missing delete", err)
	}
	if retired, err := s.FinishArtifactDelete(*work.Delete, ArtifactDeleteReceipt{Work: *work.Delete}); err != nil || !retired {
		t.Fatal(retired, err)
	}
	removed := reservationSnapshot(t, s)
	if removed.Digest == complete.Digest || len(removed.Subjects) != 0 || removed.AllocatedBytes != 2*4096 || removed.Files != 2 {
		t.Fatal("retirement failed to invalidate and release actual allowances")
	}
}

func TestArtifactReservationsMarkedRootNeverUsesInputFile(t *testing.T) {
	for _, neverCreated := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned-candidate", true: "positive-uncreated"}[neverCreated], func(t *testing.T) {
			s, j, _ := artifactRegressionWorkspace(t, false, true)
			unmarked := reservationSnapshot(t, s)
			root := reservationSubject(t, unmarked, "workspace", j.CurrentAttemptID())
			if root.RootState != 0 || root.Parent != (ArtifactFile{}) || root.Root != (ArtifactFile{}) || root.File != (ArtifactFile{}) || root.CandidateFile != (ArtifactFile{}) {
				t.Fatal("unmarked root must remain unknown")
			}
			o := maintenancePhysicalOrigin(true)
			reservation, err := s.GetWorkspaceReservation(j.ID, j.CurrentAttemptID())
			if err != nil {
				t.Fatal(err)
			}
			inputFile := reservationSubject(t, unmarked, "object", reservation.InputRef.ID).File
			if o.Root == inputFile || o.Parent == inputFile {
				t.Fatal("fixture failed distinct extracted/input roots")
			}
			if neverCreated {
				o.RootState = WorkspaceRootNeverCreated
				o.Root = ArtifactFile{}
			} else {
				o.CandidateState = WorkspacePhysicalClosedPrefix
				o.Prefix = &WorkspaceCandidatePrefix{File: ArtifactFile{17, 702}, Size: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
			}
			execution, status, completion := maintenanceFailureCallback(t, s, j, o)
			commit, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now())
			if err != nil || commit.Cleanup == nil {
				t.Fatal(commit, err)
			}
			work := *commit.Cleanup
			committed := work.Origin
			marked := reservationSnapshot(t, s)
			root = reservationSubject(t, marked, "workspace", j.CurrentAttemptID())
			if root.RootState != committed.RootState || root.Parent != committed.Parent || root.Root != committed.Root || root.CandidateID != committed.CandidateID || root.CandidateFile != committed.CandidateFile || root.Root == inputFile || root.File != (ArtifactFile{}) {
				t.Fatalf("wrong marked root projection: %+v", root)
			}
			if marked.Digest == unmarked.Digest || marked.AllocatedBytes != unmarked.AllocatedBytes || marked.Files != unmarked.Files {
				t.Fatal("marked identity must invalidate without adding allowances")
			}
			before := artifactTestUsage(t, s)
			_ = reservationSnapshot(t, s)
			if artifactTestUsage(t, s) != before {
				t.Fatal("accessor double charged Subjects")
			}
			if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{Work: work}); err != nil {
				t.Fatal(err)
			}
			if got := reservationSnapshot(t, s); got.Digest == marked.Digest {
				t.Fatal("workspace/candidate removal retained old fence")
			}
		})
	}
}

func TestArtifactReservationsCopiedAndRolledBack(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, false)
	artifactRegressionComplete(t, s, j, false)
	original := reservationSnapshot(t, s)
	// Independent known-fixture arithmetic: fixed marker/root2units, one1byte
	// completed input(rounded+2units), one1byte/1file extracted staging
	// (rounded+file+root), plus a separate32MiB/2560entry working tree.
	// Upload/object identity DTO duplicates add nothing; root is charged once.
	if original.AllocatedBytes != (2*4096)+(3*4096)+(3*4096)+(32<<20)+2560*4096 || original.Files != 2+1+2+2560 {
		t.Fatalf("independent logical allowance arithmetic: %+v", original)
	}
	mutated := original
	mutated.Subjects[0].File.Inode++
	mutated.Subjects[0].ID = "changed-copy"
	mutated.Installation.Volume++
	fresh := reservationSnapshot(t, s)
	if fresh.Digest != original.Digest || fresh.Installation.Volume != 17 || reflect.DeepEqual(fresh.Subjects, mutated.Subjects) {
		t.Fatal("returned copies aliased durable state")
	}
	rollback := errors.New("actual prospective new reservation rollback")
	before := artifactDatabaseBytes(t, s)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_, err := artifactChange(tx, maxChangeBytes, func(st *artifactState) error {
			id := strings.Repeat("d", 32)
			if _, ok := st.Uploads[id]; ok {
				return ErrArtifactConflict
			}
			now := s.now().UTC()
			st.Uploads[id] = artifactUploadRecord{Owner: "legacy", Ref: ArtifactRef{ID: id, SHA256: strings.Repeat("a", 64), Size: 1}, Revision: 1, State: "creating", Created: now, Expires: now.Add(time.Hour)}
			return nil
		})
		if err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) || !reflect.DeepEqual(reservationSnapshot(t, s), fresh) {
		t.Fatal("rolled-back identity/allowance escaped")
	}
}

func TestArtifactReservationsRefusesCorruptCountersWithoutPartialSnapshot(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	_ = artifactTestComplete(t, s, testActor())
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(artifactsBucket)
		raw := bytes.Clone(b.Get([]byte("usage")))
		binary.BigEndian.PutUint64(raw[aAllocatedBytes*8:], binary.BigEndian.Uint64(raw[aAllocatedBytes*8:])+1)
		return b.Put([]byte("usage"), raw)
	}); err != nil {
		t.Fatal(err)
	}
	before := artifactDatabaseBytes(t, s)
	out, err := s.ArtifactReservations()
	if err == nil || out.Installation.Enabled || out.Digest != ([32]byte{}) || out.Subjects != nil {
		t.Fatalf("corrupt snapshot published partial identity: %+v %v", out, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("corruption refusal repaired metadata")
	}
}

func TestArtifactReservationsStatusAndManualTransferAreNotPhysicalChanges(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, true, false)
	before := reservationSnapshot(t, s)
	artifactRegressionComplete(t, s, j, false)
	completed := reservationSnapshot(t, s)
	if completed.Digest == before.Digest || completed.AllocatedBytes != before.AllocatedBytes || completed.Files != before.Files {
		t.Fatal("atomic root identity must invalidate without adding allowance")
	}
	artifactRegressionTransferManual(t, s, j)
	manual := reservationSnapshot(t, s)
	if !reflect.DeepEqual(completed, manual) {
		t.Fatal("pin/job-to-manual transfer changed unchanged physical subjects")
	}
	if _, err := s.Get(j.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("fixture did not transfer real job", err)
	}
	_ = maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
	marked := reservationSnapshot(t, s)
	if marked.Digest != manual.Digest {
		t.Fatal("inspection changed already committed root facts")
	}
	if marked.AllocatedBytes != manual.AllocatedBytes || marked.Files != manual.Files {
		t.Fatal("manual marked root charged allowance twice")
	}
}

// NotAttempted is logical CREATE-not-entered evidence for one exact remote
// reservation, not an empty-filesystem observation. All native and File receipts
// below are MODELED; the Store/auth transactions and read-only snapshots are real.
func TestArtifactReservationsRemoteNotAttemptedLifecycle(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	j := f.submit(t, "reservation-create-boundary", f.task(t))
	key, start, hashes := f.begin(t, j)
	row := remoteTransactionGet(t, f.s, key)
	if row.Active == nil || row.Active.ResultID == "" {
		t.Fatal("fixture has no original remote output reservation")
	}
	id := row.Active.ResultID
	usage := artifactTestUsage(t, f.s)
	initial := reservationSnapshot(t, f.s)
	reserved := reservationSubject(t, initial, "object", id)
	if !reserved.NotAttempted || reserved.File != (ArtifactFile{}) {
		t.Fatalf("original CREATE-not-entered reservation: %+v", reserved)
	}
	if initial.AllocatedBytes != usage[aAllocatedBytes] || initial.Files != usage[aFiles] || usage != artifactTestUsage(t, f.s) {
		t.Fatal("logical NotAttempted fact altered prepaid counters")
	}
	if fresh := reservationSnapshot(t, f.s); !reflect.DeepEqual(fresh, initial) {
		t.Fatal("repeated validated projection changed digest or counters")
	}
	copy := initial
	copy.Subjects = append([]ArtifactPhysicalSubject(nil), initial.Subjects...)
	for i := range copy.Subjects {
		if copy.Subjects[i].Kind == "object" && copy.Subjects[i].ID == id {
			copy.Subjects[i].NotAttempted = false
		}
	}
	if !reservationSubject(t, reservationSnapshot(t, f.s), "object", id).NotAttempted {
		t.Fatal("caller changed durable NotAttempted through copied DTO")
	}
	copy.Digest = [32]byte{}
	raw, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	changedDigest := sha256.Sum256(append([]byte("skygo-artifact-reservations-v1\x00"), raw...))
	if changedDigest == initial.Digest {
		t.Fatal("canonical reservation digest did not bind NotAttempted")
	}

	row, err = f.s.RecordRemoteCreated(key, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	stop, collector := remoteTransactionStop(f, 0)
	zero := int32(0)
	row, err = f.s.ObserveRemote(key, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	beforeCreate := reservationSnapshot(t, f.s)
	if !reflect.DeepEqual(beforeCreate, initial) || artifactTestUsage(t, f.s) != usage {
		t.Fatal("native/status-only metadata changed untouched physical reservation")
	}
	grant, err := f.s.CheckRemoteGrant(key.AttemptID, RemoteOutput, hashes.Output, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	upload, err := f.s.BeginRemoteOutput(grant, strings.Repeat("d", 64), 1, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	creating := reservationSnapshot(t, f.s)
	object := reservationSubject(t, creating, "object", id)
	part := reservationSubject(t, creating, "upload", id)
	if object.NotAttempted || part.NotAttempted || object.File != (ArtifactFile{}) || part.File != (ArtifactFile{}) {
		t.Fatalf("zero File after CREATE admission acquired absence proof: object=%+v upload=%+v", object, part)
	}
	if creating.Digest == initial.Digest {
		t.Fatal("CREATE entry failed to invalidate original physical cohort")
	}
	creatingUsage := artifactTestUsage(t, f.s)
	_ = reservationSnapshot(t, f.s)
	if creatingUsage != artifactTestUsage(t, f.s) {
		t.Fatal("creating projection changed counters")
	}
	// The original part-create return is modeled explicitly. Zero/nonzero File
	// never substitutes for the CREATE admission boundary.
	file := ArtifactFile{Device: 17, Inode: 2901}
	_, err = f.s.RemotePartCreated(grant, ArtifactCursor{ID: id, Revision: upload.Revision}, file, f.now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	writable := reservationSnapshot(t, f.s)
	part = reservationSubject(t, writable, "upload", id)
	if part.NotAttempted || part.File != file || reservationSubject(t, writable, "object", id).NotAttempted {
		t.Fatal("known writable part retained CREATE-not-entered fact")
	}
	if writable.Digest == creating.Digest || writable.AllocatedBytes != creating.AllocatedBytes || writable.Files != creating.Files {
		t.Fatal("exact part identity failed to invalidate without double charging")
	}
}

func TestArtifactReservationsOrdinaryZeroFileIsNeverNotAttempted(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("e", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	usage := artifactTestUsage(t, s)
	snapshot := reservationSnapshot(t, s)
	p := reservationSubject(t, snapshot, "upload", u.Reference.ID)
	if p.File != (ArtifactFile{}) || p.NotAttempted {
		t.Fatalf("ordinary creating upload must remain unknown: %+v", p)
	}
	if snapshot.Files != 3 || snapshot.AllocatedBytes == 0 || usage != artifactTestUsage(t, s) {
		t.Fatal("ordinary unknown creation lost allowance or mutated counters")
	}
}

func TestArtifactReservationsRemoteNotAttemptedCorruptionRefusesWholeSnapshot(t *testing.T) {
	for _, kind := range []string{"missing-original-row", "wrong-original-result", "missing-active", "wrong-remote-state", "wrong-origin", "known-file", "known-manifest", "changed-ref", "gc-intent"} {
		t.Run(kind, func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			j := f.submit(t, "corrupt-reservation", f.task(t))
			key, _, _ := f.begin(t, j)
			row := remoteTransactionGet(t, f.s, key)
			id := row.Active.ResultID
			if !reservationSubject(t, reservationSnapshot(t, f.s), "object", id).NotAttempted {
				t.Fatal("valid original reservation fixture missing fact")
			}
			// Deliberate canonical-row corruption follows the existing fixture
			// style. No counter repair, admission bypass, filesystem call or GC
			// permission is inferred from these malformed rows.
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				root := tx.Bucket(artifactsBucket)
				objects := root.Bucket([]byte("objects"))
				var object artifactObject
				if err := artifactDecode(objects.Get([]byte(id)), &object); err != nil {
					return err
				}
				switch kind {
				case "missing-original-row":
					return root.Bucket([]byte("remote_attempts")).Delete([]byte(key.AttemptID))
				case "wrong-original-result", "missing-active", "wrong-remote-state":
					remote := root.Bucket([]byte("remote_attempts"))
					var r remoteRecord
					if err := artifactDecode(remote.Get([]byte(key.AttemptID)), &r); err != nil {
						return err
					}
					switch kind {
					case "wrong-original-result":
						r.Active.ResultID = strings.Repeat("f", 32)
					case "missing-active":
						r.Active = nil
					case "wrong-remote-state":
						r.State = 1
					}
					raw, err := remoteEncode(r)
					if err != nil {
						return err
					}
					return remote.Put([]byte(key.AttemptID), raw)
				case "wrong-origin":
					object.Origin = "result"
				case "known-file":
					object.File = ArtifactFile{Device: 17, Inode: 2902}
				case "known-manifest":
					object.Manifest.RegularFiles = 1
				case "changed-ref":
					object.Ref.SHA256 = strings.Repeat("b", 64)
				case "gc-intent":
					deletion := artifactDelete{Kind: "object", ObjectID: id, Owner: object.Owner, Revision: 1}
					raw, err := json.Marshal(deletion)
					if err != nil {
						return err
					}
					return root.Bucket([]byte("gc")).Put([]byte(strings.Repeat("c", 32)), raw)
				}
				raw, err := json.Marshal(object)
				if err != nil {
					return err
				}
				return objects.Put([]byte(id), raw)
			}); err != nil {
				t.Fatal(err)
			}
			before := artifactDatabaseBytes(t, f.s)
			out, err := f.s.ArtifactReservations()
			if err == nil || !reflect.DeepEqual(out, ArtifactReservations{}) {
				t.Fatalf("malformed reservation published partial snapshot: %+v %v", out, err)
			}
			if !bytes.Equal(before, artifactDatabaseBytes(t, f.s)) {
				t.Fatal("reservation refusal repaired corrupt metadata or counters")
			}
		})
	}
}
