package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These maximal SOURCE vectors cover the actual unchanged gc encoding. They
// have not run; no encoded length, physical bound or platform proof is claimed.
func TestWorkspaceCleanupMaximumCanonicalRecords(t *testing.T) {
	max := ^uint64(0)
	id := strings.Repeat("f", 32)
	for _, tt := range []struct {
		name string
		root WorkspaceRootState
		kind WorkspaceCandidateDisposition
	}{
		{"no-root-no-output", WorkspaceRootNeverCreated, WorkspaceNoOutputs},
		{"root-no-output", WorkspaceRootOwned, WorkspaceNoOutputs},
		{"no-root-never-candidate", WorkspaceRootNeverCreated, WorkspaceCandidateNeverCreated},
		{"root-never-candidate", WorkspaceRootOwned, WorkspaceCandidateNeverCreated},
		{"closed-unpublished", WorkspaceRootOwned, WorkspaceCandidateClosedUnpublished},
		{"published-independent", WorkspaceRootOwned, WorkspaceResultPublishedIndependent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := artifactWorkspaceDelete{Parent: ArtifactFile{max, max}, RootState: tt.root, Disposition: tt.kind}
			if tt.root == WorkspaceRootOwned {
				p.Root = ArtifactFile{max, max - 1}
			}
			if tt.kind == WorkspaceCandidateClosedUnpublished {
				p.CandidateID = id
				p.CandidateFile = ArtifactFile{max, max - 2}
				p.CandidateSize = artifactMaxSize
				p.CandidateSHA256 = strings.Repeat("f", 64)
			}
			d := artifactDelete{Owner: strings.Repeat("z", 64), Kind: "workspace", ObjectID: id, Revision: max, Workspace: &p}
			raw, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if len(raw) > 2048 {
				t.Fatalf("actual workspace intent %d exceeds unchanged2048", len(raw))
			}
			var decoded artifactDelete
			if err := artifactDecode(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if !artifactSameWorkspaceDelete(decoded.Workspace, &p) || decoded.Owner != d.Owner || decoded.ObjectID != id || decoded.Revision != max {
				t.Fatal("maximal identity failed canonical round trip")
			}
			again, err := json.Marshal(decoded)
			if err != nil || string(again) != string(raw) {
				t.Fatal("canonical representation changed", err)
			}
			t.Logf("gc maximal format vector %s bytes=%d key=32 reserved=2080", tt.name, len(raw))
		})
	}
}

func TestWorkspaceCleanupPrepaidAtActualBegin(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	input := artifactTestComplete(t, s, testActor())
	j, err := s.SubmitWorkspace(testActor(), nil, "prepaid-root", artifactInputTask(input), "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	before := artifactTestUsage(t, s)
	j, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, s.now())
	if err != nil || !start {
		t.Fatal(start, err)
	}
	after := artifactTestUsage(t, s)
	// Independent declared-record arithmetic: one4096+32 workspace, one1024+32
	// completion reserve and one2048+32 root-intent reserve, no result object.
	if after[aMetadataBytes]-before[aMetadataBytes] != (4096+32)+(1024+32)+(2048+32) || after[aMetadataRecords]-before[aMetadataRecords] != 3 {
		t.Fatal("Begin did not prepay separate root intent", before, after)
	}
	if after[aWorkspaces] != before[aWorkspaces]+1 {
		t.Fatal("Begin missing workspace count")
	}
	// Unknown writer closure cannot turn these prepaid bytes into authority.
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		_, err = artifactCleanupSubject(st, j.ID, j.CurrentAttemptID())
		return err
	}); err == nil {
		t.Fatal("starting attempt authorized cleanup")
	}
	if _, found, err := s.NextArtifactCleanup(""); err != nil || found {
		t.Fatal("starting attempt yielded committed cleanup", found, err)
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupInvalidPayloadCannotExploitPrepayment(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, true)
	o := maintenancePhysicalOrigin(true)
	o.CandidateState = WorkspacePhysicalClosedPrefix
	o.Prefix = &WorkspaceCandidatePrefix{File: ArtifactFile{17, 702}, SHA256: strings.Repeat("f", 64)}
	execution, status, completion := maintenanceFailureCallback(t, s, j, o)
	for _, change := range []func(*WorkspacePhysicalOrigin){
		func(o *WorkspacePhysicalOrigin) { o.Prefix.ID = strings.Repeat("d", 32) },
		func(o *WorkspacePhysicalOrigin) { o.Prefix.SHA256 = strings.Repeat("a", 2049) },
		func(o *WorkspacePhysicalOrigin) { o.Prefix.Size = -1 },
		func(o *WorkspacePhysicalOrigin) { o.Prefix.Size = artifactMaxSize + 1 },
		func(o *WorkspacePhysicalOrigin) { o.Prefix.File = o.Parent },
		func(o *WorkspacePhysicalOrigin) { o.RootState = WorkspaceRootNeverCreated; o.Root = ArtifactFile{} },
	} {
		bad := *completion.Origin
		prefix := *bad.Prefix
		bad.Prefix = &prefix
		change(&bad)
		callback := completion
		callback.Origin = &bad
		before := artifactDatabaseBytes(t, s)
		if _, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, callback, s.now()); err == nil {
			t.Fatal("invalid payload consumed prepaid slot")
		}
		if string(before) != string(artifactDatabaseBytes(t, s)) {
			t.Fatal("invalid payload changed durable state")
		}
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		if len(st.Deletes) != 0 {
			t.Fatal("invalid payload created intent")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupMaximalInodesInValidSubject(t *testing.T) {
	o := maintenancePhysicalOrigin(true)
	max := ^uint64(0)
	o.Parent.Inode = max
	o.Root.Inode = max - 1
	o.CandidateState = WorkspacePhysicalClosedPrefix
	o.Prefix = &WorkspaceCandidatePrefix{File: ArtifactFile{17, max - 2}, Size: artifactMaxSize, SHA256: strings.Repeat("f", 64)}
	s, j := maintenanceFailedOrigin(t, false, o)
	work := maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		d := st.Deletes[work.ID]
		raw, e := json.Marshal(d)
		if e != nil {
			return e
		}
		if len(raw) > 2048 || len(work.ID) != 32 {
			t.Fatal("actual maximal persisted record exceeds declared reservation")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{work}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupPositiveNeverCreatedRoot(t *testing.T) {
	o := maintenancePhysicalOrigin(true)
	// MODELED positive original sequencer facts: no root creation was attempted,
	// no candidate creation was attempted. Absence is not queried or inferred.
	o.RootState = WorkspaceRootNeverCreated
	o.Root = ArtifactFile{}
	s, j := maintenanceFailedOrigin(t, false, o)
	work := maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
	before := artifactTestUsage(t, s)
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{work}); err != nil {
		t.Fatal(err)
	}
	after := artifactTestUsage(t, s)
	if after[aWorkspaces] != 0 || after[aCompletedBytes] != before[aCompletedBytes]-artifactMaxSize {
		t.Fatal("positive uncreated reservation did not settle")
	}
}
