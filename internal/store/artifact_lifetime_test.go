package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func artifactKnownProducer() OriginalServerLifetime {
	return OriginalServerLifetime{Host: strings.Repeat("a", 32), Boot: strings.Repeat("b", 32), NamespaceDevice: 4, NamespaceInode: 5, PID: 6, StartTicks: 7}
}

// UNRUN actual Store ownership/CAS tests; no process/host/death observation.
func TestArtifactDownloadOptionalProducerAndOriginalClosure(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable-live", true: "known-live"}[known], func(t *testing.T) {
			s := openTest(t)
			artifactTestInstall(t, s)
			ref := artifactTestComplete(t, s, testActor())
			var producer OriginalServerLifetime
			if known {
				producer = artifactKnownProducer()
			}
			lease, err := s.BeginArtifactDownload(testActor(), ref, producer)
			if err != nil || lease.Producer != producer {
				t.Fatal(lease, err)
			}
			usage := artifactTestUsage(t, s)
			if usage[aDownloads] != 1 {
				t.Fatal("admission lost durable protection")
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, e := artifactLoad(tx)
				if e != nil {
					return e
				}
				r := st.Reads[lease.ID]
				if (r.Producer != nil) != known {
					t.Fatal("unavailable producer was fabricated")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			closure := ArtifactDownloadClosure{Owner: "legacy", LeaseID: lease.ID, Revision: lease.Revision, Ref: ref, File: lease.File, RootID: lease.RootID, Producer: producer}
			bad := closure
			bad.Producer.PID++
			before := artifactDatabaseBytes(t, s)
			if _, err := s.MarkArtifactDownloadClosing(bad); !errors.Is(err, ErrArtifactConflict) {
				t.Fatal("wrong original identity closed lease", err)
			}
			if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
				t.Fatal("conflicting closure mutated reader")
			}
			closure, err = s.MarkArtifactDownloadClosing(closure)
			if err != nil {
				t.Fatal(err)
			}
			bad = closure
			bad.Producer = artifactKnownProducer()
			bad.Producer.StartTicks++
			if err := s.FinishArtifactDownload(bad, s.now()); !errors.Is(err, ErrArtifactConflict) {
				t.Fatal("changed producer credited reader", err)
			}
			if artifactTestUsage(t, s)[aDownloads] != 1 {
				t.Fatal("failed closure credited lease")
			}
			// MODELED original I/O/HTTP/FD joins; zero lifetime adds no shortcut.
			if err := s.FinishArtifactDownload(closure, s.now()); err != nil {
				t.Fatal(err)
			}
			if artifactTestUsage(t, s)[aDownloads] != 0 {
				t.Fatal("positive live closure failed to credit lease")
			}
			var closedAt time.Time
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, e := artifactLoad(tx)
				if e != nil {
					return e
				}
				closedAt = st.Reads[lease.ID].ClosedAt
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.FinishArtifactDownload(closure, s.now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, e := artifactLoad(tx)
				if e != nil {
					return e
				}
				if !st.Reads[lease.ID].ClosedAt.Equal(closedAt) {
					t.Fatal("duplicate renewed original closure")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArtifactDownloadMalformedProducerRefusesBeforeAdmission(t *testing.T) {
	for _, producer := range []OriginalServerLifetime{
		{PID: 1},
		{Host: strings.Repeat("a", 32), Boot: strings.Repeat("b", 32), PID: 1},
		{Host: strings.Repeat("0", 32), Boot: strings.Repeat("b", 32), NamespaceInode: 1, PID: 1},
		{Host: strings.Repeat("a", 32), Boot: strings.Repeat("b", 32), NamespaceInode: 1, PID: 1 << 31},
	} {
		s := openTest(t)
		artifactTestInstall(t, s)
		ref := artifactTestComplete(t, s, testActor())
		before := artifactDatabaseBytes(t, s)
		if _, err := s.BeginArtifactDownload(testActor(), ref, producer); !errors.Is(err, ErrArtifactConflict) {
			t.Fatal("malformed identity admitted", producer, err)
		}
		if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
			t.Fatal("refused admission wrote rows")
		}
	}
}

func TestArtifactProducerCanonicalNilAndMaximumEncoding(t *testing.T) {
	max := ^uint64(0)
	producer := OriginalServerLifetime{Host: strings.Repeat("a", 32), Boot: strings.Repeat("b", 32), NamespaceDevice: max, NamespaceInode: max, PID: 2147483647, StartTicks: max}
	// Literal values and field ordering are independent of reflection over the
	// production record. Encoding, canonical decode and strict shape all apply.
	wantProducer := `{"Host":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","Boot":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","NamespaceDevice":18446744073709551615,"NamespaceInode":18446744073709551615,"PID":2147483647,"StartTicks":18446744073709551615}`
	raw, err := json.Marshal(producer)
	if err != nil || string(raw) != wantProducer || !producer.valid() {
		t.Fatal(string(raw), err)
	}
	var decoded OriginalServerLifetime
	if err := artifactDecode(raw, &decoded); err != nil || decoded != producer {
		t.Fatal(decoded, err)
	}
	late := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("max", 23*3600+59*60))
	ref := ArtifactRef{ID: strings.Repeat("f", 32), SHA256: strings.Repeat("f", 64), Size: 33554432}
	r := artifactRead{Owner: strings.Repeat("z", 64), Ref: ref, Revision: 2, Expires: late, Closing: true, Closed: true, ClosedAt: late, Producer: &producer}
	raw, err = json.Marshal(r)
	if err != nil || len(raw) > 1024 {
		t.Fatal(len(raw), err)
	}
	var read artifactRead
	if err := artifactDecode(raw, &read); err != nil || read.Producer == nil || *read.Producer != producer || read.Revision != 2 {
		t.Fatal(read, err)
	}
	again, err := json.Marshal(read)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("maximal read lost canonical bytes", err)
	}
	// This actual valid-revision record exercises the conservative timestamp
	// envelope; maxUint64 read revision is tested separately as corruption.
	if len(raw) > 580 {
		t.Fatal("valid closed-read exceeds independently counted envelope", len(raw))
	}
	r.Producer = nil
	raw, err = json.Marshal(r)
	if err != nil || bytes.Contains(raw, []byte(`"Producer"`)) {
		t.Fatal("unknown identity changed omitted representation", err)
	}
	p := artifactWorkspaceDelete{Parent: ArtifactFile{max, max}, Root: ArtifactFile{max, max - 1}, RootState: WorkspaceRootOwned, Disposition: WorkspaceCandidateClosedUnpublished, CandidateID: ref.ID, CandidateFile: ArtifactFile{max, max - 2}, CandidateSize: 33554432, CandidateSHA256: ref.SHA256, Producer: &producer}
	d := artifactDelete{Owner: r.Owner, Kind: "workspace", ObjectID: ref.ID, Revision: max, Workspace: &p}
	raw, err = json.Marshal(d)
	if err != nil || len(raw) > 2048 {
		t.Fatal(len(raw), err)
	}
	var deletion artifactDelete
	if err := artifactDecode(raw, &deletion); err != nil || !artifactSameWorkspaceDelete(deletion.Workspace, &p) {
		t.Fatal("maximal gc canonical round trip", err)
	}
	again, err = json.Marshal(deletion)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("gc encoding changed", err)
	}
	if len(raw) != 846 {
		t.Fatal("actual maximal gc differs from independently counted complete shape", len(raw))
	}
	// Exercise the actual metadata relationship validator, not only encoding.
	st := &artifactState{Root: artifactRoot{Volume: max}, Workspaces: map[string]artifactWorkspace{ref.ID: {Owner: r.Owner, JobID: ref.ID, AttemptID: ref.ID, Instance: "i", WriterClosed: true, WriterReceipt: [32]byte{1}, ResultID: ref.ID}}, Completions: map[string]artifactCompletion{ref.ID: {Owner: r.Owner, JobID: ref.ID, AttemptID: ref.ID, TaskDigest: [32]byte{1}, Digest: [32]byte{2}}}, Objects: map[string]artifactObject{ref.ID: {Owner: r.Owner, JobID: ref.ID, Source: ref.ID, Origin: "result", State: "reserved"}}}
	if err := artifactWorkspaceDeleteValid(st, d); err != nil {
		t.Fatal("maximal workspace relationship refused", err)
	}
	// Physical geometry, core membership and native reachability are not
	// established by this finite maximum-format relationship vector.
}

func TestArtifactProducerDeepCopiesAndCorruptReadRefusal(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	ref := artifactTestComplete(t, s, testActor())
	producer := artifactKnownProducer()
	lease, err := s.BeginArtifactDownload(testActor(), ref, producer)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		copy := st.clone()
		copy.Reads[lease.ID].Producer.PID++
		if *st.Reads[lease.ID].Producer != producer {
			t.Fatal("clone aliased immutable producer")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		change func(*artifactRead)
	}{
		{"invalid-revision", func(r *artifactRead) { r.Revision = ^uint64(0) }},
		{"nonnil-zero", func(r *artifactRead) { r.Producer = &OriginalServerLifetime{} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rollback := errors.New("keep corruption private")
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(artifactsBucket).Bucket([]byte("reads"))
				var r artifactRead
				if err := artifactDecode(b.Get([]byte(lease.ID)), &r); err != nil {
					return err
				}
				tt.change(&r)
				raw, err := json.Marshal(r)
				if err != nil {
					return err
				}
				if err := b.Put([]byte(lease.ID), raw); err != nil {
					return err
				}
				if err := validateSnapshot(tx); err == nil {
					t.Fatal("corrupt read validated")
				}
				return rollback
			}); !errors.Is(err, rollback) {
				t.Fatal(err)
			}
		})
	}
}
