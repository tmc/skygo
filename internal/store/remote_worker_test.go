package store

import (
	"reflect"

	"encoding/json"
	"errors"
	"github.com/tmc/skygo/internal/backend"
	"strings"
	"testing"
	"time"
)

// These tests exercise Store identity/metadata fences. They do not establish
// original HTTP joins, native cessation, file publication or live Pod behavior.
func TestRemoteUploadCannotUseOrdinaryOwnerCursor(t *testing.T) {
	id := strings.Repeat("a", 32)
	u := artifactUploadRecord{Owner: "owner", Ref: ArtifactRef{ID: id}, Revision: 4, RemoteAttemptID: strings.Repeat("b", 32)}
	st := &artifactState{Uploads: map[string]artifactUploadRecord{id: u}}
	if _, err := artifactOwnedUpload(st, "owner", ArtifactCursor{ID: id, Revision: 4}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ordinary owner adopted scoped cursor: %v", err)
	}
	u.RemoteAttemptID = ""
	st.Uploads[id] = u
	if got, err := artifactOwnedUpload(st, "owner", ArtifactCursor{ID: id, Revision: 4}); err != nil || got != u {
		t.Fatalf("ordinary cursor changed: %+v, %v", got, err)
	}
}

func TestRemotePrivateCandidateRemainsPinned(t *testing.T) {
	id := strings.Repeat("a", 32)
	attempt := strings.Repeat("b", 32)
	for _, state := range []uint8{1, 2, 3, 4} {
		st := &artifactState{Remote: map[string]remoteRecord{attempt: {State: state, Active: &remoteActive{ResultID: id}}}}
		if got, want := st.pinned(id), state == 2; got != want {
			t.Fatalf("state %d pinned=%v, want %v", state, got, want)
		}
		if st.pinned(strings.Repeat("c", 32)) {
			t.Fatal("unrelated object pinned")
		}
	}
}

func TestRemoteOptionalFieldsPreserveLegacyEncoding(t *testing.T) {
	// Explicit old shapes pin field order as well as omission. A zero remote tag
	// must not change a single byte of existing canonical persisted rows.
	type oldUpload struct {
		Owner      string
		Ref        ArtifactRef
		Revision   uint64
		State      string
		Ack        uint64
		Created    time.Time
		Expires    time.Time
		Part       ArtifactFile
		LastOffset uint64
		LastSize   uint64
		LastSHA256 string
	}
	type oldRead struct {
		Owner    string
		Ref      ArtifactRef
		Revision uint64
		Expires  time.Time
		Closing  bool
		Closed   bool
		ClosedAt time.Time
		Producer *OriginalServerLifetime `json:",omitempty"`
	}
	now := time.Date(2026, 10, 20, 1, 2, 3, 4, time.UTC)
	ref := ArtifactRef{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 9}
	u := artifactUploadRecord{Owner: "owner", Ref: ref, Revision: 7, State: "writable", Ack: 4, Created: now, Expires: now.Add(time.Hour), Part: ArtifactFile{Device: 7, Inode: 8}, LastSize: 4, LastSHA256: strings.Repeat("c", 64)}
	oldU := oldUpload{u.Owner, u.Ref, u.Revision, u.State, u.Ack, u.Created, u.Expires, u.Part, u.LastOffset, u.LastSize, u.LastSHA256}
	read := artifactRead{Owner: "owner", Ref: ref, Revision: 3, Expires: now, Closing: true}
	oldR := oldRead{read.Owner, read.Ref, read.Revision, read.Expires, read.Closing, read.Closed, read.ClosedAt, read.Producer}
	for _, tc := range []struct {
		name     string
		old, now any
	}{{"upload", oldU, u}, {"read", oldR, read}} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := json.Marshal(tc.old)
			if err != nil {
				t.Fatal(err)
			}
			after, err := json.Marshal(tc.now)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatalf("legacy bytes changed:\n%s\n%s", before, after)
			}
		})
	}
}

func TestRemoteReadClosurePreservesOriginalIdentity(t *testing.T) {
	id := strings.Repeat("a", 32)
	object := strings.Repeat("b", 32)
	attempt := strings.Repeat("c", 32)
	ref := ArtifactRef{ID: object, SHA256: strings.Repeat("d", 64), Size: 1}
	file := ArtifactFile{Device: 7, Inode: 8}
	st := &artifactState{Root: artifactRoot{ID: strings.Repeat("e", 32)}, Reads: map[string]artifactRead{id: {Owner: "owner", Ref: ref, Revision: 2, Closing: true, RemoteAttemptID: attempt}}, Objects: map[string]artifactObject{object: {Owner: "owner", Ref: ref, File: file}}}
	closure := ArtifactDownloadClosure{Owner: "owner", LeaseID: id, Revision: 2, Ref: ref, File: file, RootID: st.Root.ID}
	if _, err := artifactMatchDownload(st, closure); err != nil {
		t.Fatal(err)
	}
	wrong := closure
	wrong.File.Inode++
	if _, err := artifactMatchDownload(st, wrong); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("changed inode accepted: %v", err)
	}
	now := time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC)
	if err := artifactReaderClosed(st, "owner", ArtifactCursor{ID: id, Revision: 2}, now); err != nil {
		t.Fatal(err)
	}
	read := st.Reads[id]
	if !read.Closed || read.RemoteAttemptID != attempt || !read.ClosedAt.Equal(now) {
		t.Fatalf("closure lost scope or receipt: %+v", read)
	}
	if err := artifactReaderClosed(st, "owner", ArtifactCursor{ID: id, Revision: 2}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !st.Reads[id].ClosedAt.Equal(now) {
		t.Fatal("duplicate closure changed first receipt")
	}
}

func TestRemoteUploadRequiresExactScopeAndOriginalCursor(t *testing.T) {
	now := time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC)
	job := strings.Repeat("a", 32)
	attempt := strings.Repeat("b", 32)
	id := strings.Repeat("c", 32)
	until := now.Add(time.Minute)
	grant := RemoteGrant{Key: RemoteKey{JobID: job, AttemptID: attempt}, Kind: RemoteOutput, Epoch: 7, Expires: until}
	cursor := ArtifactCursor{ID: id, Revision: 9}
	fresh := func() *artifactState {
		return &artifactState{
			Remote: map[string]remoteRecord{
				job:     {Job: job, Authority: &remoteAuthority{Principal: "owner"}},
				attempt: {Job: job, State: 2, Creation: 3, Active: &remoteActive{Phase: 4, ResourceDeadline: now.Add(time.Hour), ReleaseAt: &now, Budgets: [4]int64{600, 86400, 600, 120}, Output: &remoteLease{State: 2, Epoch: 7, Expires: until}, Stop: &remoteTaskStop{Task: remoteExit{Code: 0}}, ReleaseRevision: 5, ResultID: id}},
			},
			Uploads: map[string]artifactUploadRecord{id: {RemoteAttemptID: attempt, Owner: "owner", Ref: ArtifactRef{ID: id}, Revision: 9, State: "writable", Expires: until}},
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*artifactState, *RemoteGrant, *ArtifactCursor)
	}{
		{"epoch", func(_ *artifactState, g *RemoteGrant, _ *ArtifactCursor) { g.Epoch++ }},
		{"expiry", func(_ *artifactState, g *RemoteGrant, _ *ArtifactCursor) { g.Expires = g.Expires.Add(time.Nanosecond) }},
		{"job", func(_ *artifactState, g *RemoteGrant, _ *ArtifactCursor) { g.Key.JobID = strings.Repeat("d", 32) }},
		{"cursor", func(_ *artifactState, _ *RemoteGrant, c *ArtifactCursor) { c.Revision++ }},
		{"reservation", func(_ *artifactState, _ *RemoteGrant, c *ArtifactCursor) { c.ID = strings.Repeat("d", 32) }},
		{"input-purpose", func(_ *artifactState, g *RemoteGrant, _ *ArtifactCursor) { g.Kind = RemoteInput }},
		{"unactivated-success", func(st *artifactState, _ *RemoteGrant, _ *ArtifactCursor) {
			r := st.Remote[attempt]
			r.Creation = 1
			st.Remote[attempt] = r
		}},
		{"original-error", func(st *artifactState, _ *RemoteGrant, _ *ArtifactCursor) {
			r := st.Remote[attempt]
			r.Creation = 2
			st.Remote[attempt] = r
		}},
		{"closed", func(st *artifactState, _ *RemoteGrant, _ *ArtifactCursor) { st.Remote[attempt].Active.Output.State = 3 }},
		{"no-release", func(st *artifactState, _ *RemoteGrant, _ *ArtifactCursor) {
			st.Remote[attempt].Active.ReleaseRevision = 0
		}},
		{"main-failed", func(st *artifactState, _ *RemoteGrant, _ *ArtifactCursor) {
			st.Remote[attempt].Active.Stop.Task.Code = 1
		}},
		{"foreign-upload", func(st *artifactState, _ *RemoteGrant, _ *ArtifactCursor) {
			u := st.Uploads[id]
			u.RemoteAttemptID = job
			st.Uploads[id] = u
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := fresh()
			g, c := grant, cursor
			tc.change(st, &g, &c)
			if _, _, err := remoteUpload(st, g, c, now); err == nil {
				t.Fatal("invalid scope or cursor admitted")
			}
		})
	}
	if _, _, err := remoteUpload(fresh(), grant, cursor, now); err != nil {
		t.Fatalf("valid scoped cursor refused: %v", err)
	}
	if _, _, err := remoteUpload(fresh(), grant, cursor, until); err == nil {
		t.Fatal("deadline equality admitted")
	}
}

// This public transaction oracle models original local server File/archive
// receipts. It opens no corresponding part/publication file and proves neither
// native cessation nor physical hashing, prefix equality or owner joins.
func TestRemoteOutputPublicReplayAndConflicts(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	j := f.submit(t, "output-replay", f.task(t))
	key, start, hashes := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(key, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	stop, collector := remoteTransactionStop(f, 0)
	zero := int32(0)
	now := f.now.Add(3 * time.Second)
	row, err = f.s.ObserveRemote(key, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, now)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := f.s.CheckRemoteGrant(key.AttemptID, RemoteOutput, hashes.Output, now)
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("d", 64)
	size := uint64(2*artifactChunkSize + 17)
	upload, err := f.s.BeginRemoteOutput(grant, sha, size, now)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Reference.ID != row.Active.ResultID || upload.Reference.Size != size || upload.Reference.SHA256 != sha {
		t.Fatalf("lost original reservation: %#v", upload)
	}
	replayBegin := func(want ArtifactUpload) {
		t.Helper()
		before := remoteTransactionDatabaseBytes(t, f.s)
		got, e := f.s.BeginRemoteOutput(grant, sha, size, now.Add(time.Second))
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("begin exact replay: %#v, %v; want %#v", got, e, want)
		}
		remoteTransactionDatabaseUnchanged(t, f.s, before)
	}
	replayBegin(upload)
	for _, tc := range []struct {
		name, digest string
		size         uint64
	}{
		{"changed-digest", strings.Repeat("e", 64), size},
		{"changed-size", sha, size + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := remoteTransactionDatabaseBytes(t, f.s)
			if _, e := f.s.BeginRemoteOutput(grant, tc.digest, tc.size, now); e == nil {
				t.Fatal("changed original declaration accepted")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
		})
	}
	// MODELED positive receipt from the same original server part owner.
	file := ArtifactFile{Device: 17, Inode: 1800}
	upload, err = f.s.RemotePartCreated(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, file, now)
	if err != nil {
		t.Fatal(err)
	}
	prefixSHA := strings.Repeat("e", 64)
	upload, err = f.s.RemoteChunkSynced(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, file, 0, artifactChunkSize, prefixSHA, now)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Acknowledged != artifactChunkSize {
		t.Fatalf("prefix not acknowledged: %#v", upload)
	}
	cursor := ArtifactCursor{upload.Reference.ID, upload.Revision}
	before := remoteTransactionDatabaseBytes(t, f.s)
	again, err := f.s.RemoteChunkSynced(grant, cursor, file, 0, artifactChunkSize, prefixSHA, now.Add(time.Second))
	if err != nil || !reflect.DeepEqual(again, upload) {
		t.Fatalf("current-cursor exact prefix retransmit: %#v, %v", again, err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	for _, tc := range []struct {
		name         string
		file         ArtifactFile
		offset, size uint64
		sha          string
	}{
		{"changed-committed-prefix", file, 0, artifactChunkSize, strings.Repeat("f", 64)},
		{"changed-prefix-size", file, 0, artifactChunkSize - 1, prefixSHA},
		{"hole", file, upload.Acknowledged + 1, 1, prefixSHA},
		{"offset-overflow", file, ^uint64(0), 1, prefixSHA},
		{"size-overflow", file, upload.Acknowledged, ^uint64(0), prefixSHA},
		{"wrong-original-file", ArtifactFile{Device: 17, Inode: 1801}, upload.Acknowledged, 1, prefixSHA},
		{"zero-size", file, upload.Acknowledged, 0, prefixSHA},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := remoteTransactionDatabaseBytes(t, f.s)
			if _, e := f.s.RemoteChunkSynced(grant, cursor, tc.file, tc.offset, tc.size, tc.sha, now); e == nil {
				t.Fatal("conflicting prefix admitted")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
		})
	}
	before = remoteTransactionDatabaseBytes(t, f.s)
	if _, err = f.s.BeginRemoteSeal(grant, cursor, now); err == nil {
		t.Fatal("incomplete prefix sealed")
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	upload, err = f.s.RemoteChunkSynced(grant, cursor, file, artifactChunkSize, artifactChunkSize, strings.Repeat("a", 64), now)
	if err != nil {
		t.Fatal(err)
	}
	cursor = ArtifactCursor{upload.Reference.ID, upload.Revision}
	// Each individual chunk is bounded; reject an otherwise contiguous suffix
	// that would exceed the declared object size, without committing a prefix.
	before = remoteTransactionDatabaseBytes(t, f.s)
	if _, err = f.s.RemoteChunkSynced(grant, cursor, file, upload.Acknowledged, 18, strings.Repeat("b", 64), now); err == nil {
		t.Fatal("declared-size overflow admitted")
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	upload, err = f.s.RemoteChunkSynced(grant, cursor, file, upload.Acknowledged, 17, strings.Repeat("b", 64), now)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Acknowledged != size {
		t.Fatalf("full prefix not durable: %#v", upload)
	}
	replayBegin(upload)
	upload, err = f.s.BeginRemoteSeal(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, now)
	if err != nil {
		t.Fatal(err)
	}
	cursor = ArtifactCursor{upload.Reference.ID, upload.Revision}
	before = remoteTransactionDatabaseBytes(t, f.s)
	again, err = f.s.BeginRemoteSeal(grant, cursor, now.Add(time.Second))
	if err != nil || !reflect.DeepEqual(again, upload) {
		t.Fatalf("current-cursor begin-seal replay: %#v, %v", again, err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	// MODELED validated exclusive publication and archive receipt from the same
	// server owner. It is not an inode observed on the remote Pod.
	seal := RemoteOutputSeal{Reference: upload.Reference, File: ArtifactFile{Device: 17, Inode: 1900}, Manifest: ArtifactManifest{Reference: upload.Reference, ExpandedBytes: size, RegularFiles: 2, Directories: 1}}
	if err = f.s.SealRemoteOutput(grant, cursor, seal, now); err != nil {
		t.Fatal(err)
	}
	// Obtain the committed current cursor through the public exact Begin replay;
	// never mistake rejection of the stale pre-seal revision for receipt fencing.
	sealed, err := f.s.BeginRemoteOutput(grant, sha, size, now)
	if err != nil || sealed.State != "complete" || sealed.Revision <= upload.Revision {
		t.Fatalf("sealed cursor: %#v, %v", sealed, err)
	}
	replayBegin(sealed)
	cursor = ArtifactCursor{sealed.Reference.ID, sealed.Revision}
	before = remoteTransactionDatabaseBytes(t, f.s)
	if err = f.s.SealRemoteOutput(grant, cursor, seal, now.Add(time.Second)); err != nil {
		t.Fatalf("current-cursor exact seal replay: %v", err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	for _, tc := range []struct {
		name   string
		change func(*RemoteOutputSeal)
	}{
		{"changed-publication-inode", func(s *RemoteOutputSeal) { s.File.Inode++ }},
		{"changed-valid-manifest", func(s *RemoteOutputSeal) { s.Manifest.RegularFiles++ }},
		{"changed-content-ref", func(s *RemoteOutputSeal) {
			s.Reference.SHA256 = strings.Repeat("f", 64)
			s.Manifest.Reference = s.Reference
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := seal
			tc.change(&changed)
			before := remoteTransactionDatabaseBytes(t, f.s)
			if e := f.s.SealRemoteOutput(grant, cursor, changed, now); e == nil {
				t.Fatal("conflicting original seal receipt accepted")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
		})
	}
	result, err := f.s.ArtifactResultFor(f.actor, j.ID, key.AttemptID)
	if err != nil || result.State != "absent" || result.Reference != nil {
		t.Fatalf("private candidate became attached: %#v, %v", result, err)
	}
	before = remoteTransactionDatabaseBytes(t, f.s)
	if _, err = f.s.BeginArtifactDownload(f.actor, seal.Reference, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("private candidate acquired owner grant: %v", err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
}

// newRemoteOutputSnapshotFixture uses public Store/auth transactions. Native
// observations and original physical server receipts remain MODELED: no Pod,
// archive validation, part file, FD closure or HTTP join is proved here.
func newRemoteOutputSnapshotFixture(t *testing.T) (*remoteTransactionFixture, RemoteKey, RemoteGrant, backend.DataTaskStop, backend.ContainerExit, time.Time) {
	t.Helper()
	f := newRemoteTransactionFixture(t)
	j := f.submit(t, "output-snapshot", f.task(t))
	key, start, hashes := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(key, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	stop, collector := remoteTransactionStop(f, 0)
	zero := int32(0)
	now := f.now.Add(3 * time.Second)
	if _, err = f.s.ObserveRemote(key, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, now); err != nil {
		t.Fatal(err)
	}
	grant, err := f.s.CheckRemoteGrant(key.AttemptID, RemoteOutput, hashes.Output, now)
	if err != nil {
		t.Fatal(err)
	}
	return f, key, grant, stop, collector, now
}

func remoteOutputSnapshotRefused(t *testing.T, s *Store, grant RemoteGrant, now time.Time, want error) {
	t.Helper()
	before := remoteTransactionDatabaseBytes(t, s)
	upload, file, manifest, err := s.GetRemoteOutput(grant, now)
	if err == nil || want != nil && !errors.Is(err, want) {
		t.Fatalf("snapshot refusal: %v, want %v", err, want)
	}
	if upload != (ArtifactUpload{}) || file != (ArtifactFile{}) || manifest != (ArtifactManifest{}) {
		t.Fatalf("failed snapshot disclosed partial values: %#v %#v %#v", upload, file, manifest)
	}
	remoteTransactionDatabaseUnchanged(t, s, before)
}

func TestRemoteOutputSnapshotPublicLifecycle(t *testing.T) {
	f, key, grant, stop, collector, now := newRemoteOutputSnapshotFixture(t)
	remoteOutputSnapshotRefused(t, f.s, grant, now, ErrNotFound)
	upload, err := f.s.BeginRemoteOutput(grant, strings.Repeat("d", 64), 13, now)
	if err != nil {
		t.Fatal(err)
	}
	check := func(want ArtifactUpload, wantFile ArtifactFile, wantManifest ArtifactManifest) {
		t.Helper()
		before := remoteTransactionDatabaseBytes(t, f.s)
		got, file, manifest, e := f.s.GetRemoteOutput(grant, now)
		if e != nil || got != want || file != wantFile || manifest != wantManifest {
			t.Fatalf("snapshot: %#v %#v %#v, %v; want %#v %#v %#v", got, file, manifest, e, want, wantFile, wantManifest)
		}
		remoteTransactionDatabaseUnchanged(t, f.s, before)
		// All returned metadata must be detached; editing any returned value cannot
		// alter a future snapshot or turn UNKNOWN creation into an adopted inode.
		got.Reference.SHA256 = strings.Repeat("f", 64)
		got.Revision++
		file.Inode++
		manifest.RegularFiles++
		again, againFile, againManifest, e := f.s.GetRemoteOutput(grant, now)
		if e != nil || again != want || againFile != wantFile || againManifest != wantManifest {
			t.Fatalf("snapshot alias or unstable state: %#v %#v %#v, %v", again, againFile, againManifest, e)
		}
		remoteTransactionDatabaseUnchanged(t, f.s, before)
		// The genuinely authenticated ordinary Actor still cannot adopt a remote
		// upload via either owner getter, even while the worker getter succeeds.
		ordinary, e := f.s.GetArtifactUpload(f.actor, want.Reference.ID)
		if !errors.Is(e, ErrNotFound) || ordinary != (ArtifactUpload{}) {
			t.Fatalf("ordinary upload adoption: %#v %v", ordinary, e)
		}
		ordinary, ordinaryFile, e := f.s.GetArtifactUploadFile(f.actor, ArtifactCursor{want.Reference.ID, want.Revision})
		if !errors.Is(e, ErrNotFound) || ordinary != (ArtifactUpload{}) || ordinaryFile != (ArtifactFile{}) {
			t.Fatalf("ordinary file adoption: %#v %#v %v", ordinary, ordinaryFile, e)
		}
		remoteTransactionDatabaseUnchanged(t, f.s, before)
	}
	// Creating's zero File is UNKNOWN. This read must not create, repair, renew,
	// acknowledge a prefix, or infer absence from a missing physical part.
	if upload.State != "creating" {
		t.Fatalf("initial upload: %#v", upload)
	}
	check(upload, ArtifactFile{}, ArtifactManifest{})
	f.restart(t)
	check(upload, ArtifactFile{}, ArtifactManifest{})
	part := ArtifactFile{Device: 17, Inode: 2400} // MODELED original exclusive part.
	upload, err = f.s.RemotePartCreated(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, part, now)
	if err != nil {
		t.Fatal(err)
	}
	if upload.State != "writable" {
		t.Fatalf("part state: %#v", upload)
	}
	check(upload, part, ArtifactManifest{})
	upload, err = f.s.RemoteChunkSynced(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, part, 0, 13, strings.Repeat("e", 64), now)
	if err != nil {
		t.Fatal(err)
	}
	check(upload, part, ArtifactManifest{})
	upload, err = f.s.BeginRemoteSeal(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, now)
	if err != nil {
		t.Fatal(err)
	}
	if upload.State != "finalizing" {
		t.Fatalf("seal state: %#v", upload)
	}
	check(upload, part, ArtifactManifest{})
	f.restart(t)
	check(upload, part, ArtifactManifest{})
	// Publication is intentionally a DIFFERENT original opened identity from the
	// part: a completed snapshot must return the actual object receipt/manifest.
	sealedFile := ArtifactFile{Device: 17, Inode: 2500}
	manifest := ArtifactManifest{Reference: upload.Reference, ExpandedBytes: 13, RegularFiles: 2, Directories: 1}
	seal := RemoteOutputSeal{Reference: upload.Reference, File: sealedFile, Manifest: manifest}
	if err = f.s.SealRemoteOutput(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, seal, now); err != nil {
		t.Fatal(err)
	}
	upload, err = f.s.BeginRemoteOutput(grant, upload.Reference.SHA256, upload.Reference.Size, now)
	if err != nil || upload.State != "complete" {
		t.Fatalf("private complete cursor: %#v %v", upload, err)
	}
	check(upload, sealedFile, manifest)
	f.restart(t)
	check(upload, sealedFile, manifest)
	result, err := f.s.ArtifactResultFor(f.actor, key.JobID, key.AttemptID)
	if err != nil || result.State != "absent" || result.Reference != nil {
		t.Fatalf("getter attached private output: %#v %v", result, err)
	}
	// Use a valid live snapshot as the control for each exact-generation refusal.
	for _, tc := range []struct {
		name   string
		change func(*RemoteGrant)
	}{
		{"input-purpose", func(g *RemoteGrant) { g.Kind = RemoteInput }},
		{"invalid-purpose", func(g *RemoteGrant) { g.Kind = 0 }},
		{"foreign-job", func(g *RemoteGrant) { g.Key.JobID = strings.Repeat("f", 32) }},
		{"foreign-attempt", func(g *RemoteGrant) { g.Key.AttemptID = strings.Repeat("f", 32) }},
		{"stale-epoch", func(g *RemoteGrant) { g.Epoch++ }},
		{"stale-expiry", func(g *RemoteGrant) { g.Expires = g.Expires.Add(-time.Nanosecond) }},
	} {
		t.Run(tc.name, func(t *testing.T) { bad := grant; tc.change(&bad); remoteOutputSnapshotRefused(t, f.s, bad, now, nil) })
	}
	ordinary, err := f.s.BeginArtifactUpload(f.actor, strings.Repeat("c", 64), 7)
	if err != nil {
		t.Fatal(err)
	}
	foreign := grant
	foreign.Key.AttemptID = ordinary.Reference.ID
	remoteOutputSnapshotRefused(t, f.s, foreign, now, nil)
	remoteOutputSnapshotRefused(t, f.s, grant, grant.Expires, nil)
	remoteOutputSnapshotRefused(t, f.s, grant, time.Time{}, nil)
	check(upload, sealedFile, manifest)
	// The nineteenth method is not a cleanup capability. Even the SAME formerly
	// valid grant must disclose zero metadata after ordinary accepted completion.
	if _, err = f.s.CompleteRemoteAttempt(key, RemoteCompletion{Outcome: DataMainExit, Stop: &stop, Collector: &collector, Candidate: &seal}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	remoteOutputSnapshotRefused(t, f.s, grant, now.Add(time.Second), nil)
	f.restart(t)
	remoteOutputSnapshotRefused(t, f.s, grant, now.Add(time.Second), nil)
}

func TestRemoteOutputSnapshotRevokedAndDeletingRefuse(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "revoked"
		if deleting {
			name = "deleting-after-completion"
		}
		t.Run(name, func(t *testing.T) {
			f, key, grant, stop, collector, now := newRemoteOutputSnapshotFixture(t)
			upload, err := f.s.BeginRemoteOutput(grant, strings.Repeat("d", 64), 13, now)
			if err != nil {
				t.Fatal(err)
			}
			part := ArtifactFile{Device: 17, Inode: 2600} // MODELED original known part.
			upload, err = f.s.RemotePartCreated(grant, ArtifactCursor{upload.Reference.ID, upload.Revision}, part, now)
			if err != nil {
				t.Fatal(err)
			}
			got, file, manifest, err := f.s.GetRemoteOutput(grant, now)
			if err != nil || got != upload || file != part || manifest != (ArtifactManifest{}) {
				t.Fatalf("live control: %#v %#v %#v %v", got, file, manifest, err)
			}
			if deleting {
				// MODELED original transfer/FD owners have joined. Positive main Stop is
				// kept while this failed collection places the exact known part into the
				// existing deletion-intent route; no native or file deletion is simulated.
				if _, err = f.s.CompleteRemoteAttempt(key, RemoteCompletion{Outcome: DataCollectionFailure, Stop: &stop, Collector: &collector}, now.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				work, found, e := f.s.NextArtifactCleanup("")
				if e != nil || !found || work.Delete == nil || work.Delete.Kind != "upload" || work.Delete.Ref != upload.Reference || work.Delete.File != part {
					t.Fatalf("known part not on exact deleting route: %#v %v %v", work, found, e)
				}
			} else {
				row := remoteTransactionGet(t, f.s, key)
				if _, err = f.s.RevokeRemote(key, row.Revision, RemoteShutdown, now.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			remoteOutputSnapshotRefused(t, f.s, grant, now.Add(time.Second), nil)
			f.restart(t)
			remoteOutputSnapshotRefused(t, f.s, grant, now.Add(time.Second), nil)
		})
	}
}
