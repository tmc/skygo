package store

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Legal escaped fields, not only printable ASCII, exercise the new declared
// record reservations. These tests are source-only and have not been executed.
func TestArtifactIntegrationRecordBounds(t *testing.T) {
	id := strings.Repeat("a", 32)
	owner := strings.Repeat("b", 64)
	ref := ArtifactRef{ID: id, SHA256: strings.Repeat("f", 64), Size: artifactMaxSize}
	binding := strings.Repeat("\x01", 256)
	instance := strings.Repeat("<", 128)
	if !validBinding(binding) || !validInstance(instance) {
		t.Fatal("maximum legal identity changed")
	}
	late := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	var digest [32]byte
	for i := range digest {
		digest[i] = 255
	}
	exit := int32(255)
	for _, tt := range []struct {
		name   string
		record any
	}{
		{"objects", artifactObject{Owner: owner, JobID: id, Ref: ref, Origin: "result", Source: id, State: "complete", File: ArtifactFile{^uint64(0), ^uint64(0)}, Manifest: ArtifactManifest{Reference: ref, ExpandedBytes: artifactMaxSize, RegularFiles: 2048, Directories: 512}, Expires: late, DeleteAfter: late}},
		{"workspaces", artifactWorkspace{Owner: owner, JobID: id, AttemptID: id, Binding: binding, Instance: instance, Input: ref, ResultID: id, WriterClosed: true, WriterReceipt: digest, Held: true, Diagnostic: strings.Repeat("<", 128)}},
		{"completions", artifactCompletion{Owner: owner, JobID: id, AttemptID: id, TaskDigest: digest, Digest: digest, Outcome: WorkspaceTaskExit, OriginalTaskExit: &exit, RootRemoved: false}},
		{"completions", artifactCompletion{Owner: owner, JobID: id, AttemptID: id, TaskDigest: digest, Digest: digest, Outcome: WorkspaceExecutionFailure, OriginalTaskExit: nil, RootRemoved: false}},
		{"result_expired", artifactExpiredResult{Owner: owner, JobID: id, AttemptID: id, ObjectID: id, Expired: late, Collected: false}},
		{"reads", artifactRead{Owner: owner, Ref: ref, Revision: 2, Expires: late, Closing: true, Closed: true, ClosedAt: late}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.record)
			if err != nil {
				t.Fatal(err)
			}
			var limit uint64
			for _, table := range artifactTables {
				if table.name == tt.name {
					limit = table.max
				}
			}
			if limit == 0 || uint64(len(data)) > limit {
				t.Fatalf("encoded %d bytes exceeds declared %d", len(data), limit)
			}
			t.Logf("actual escaped %s bytes=%d declared=%d", tt.name, len(data), limit)
		})
	}
}
