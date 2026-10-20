package store

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// These are actual Store reads/transactions with the existing MODELED trusted
// archive/native receipts. They make no physical bytes or native closure claim.
func TestArtifactRequiredObjectsDisabledAndEmpty(t *testing.T) {
	s := openTest(t)
	if got, err := s.ArtifactRequiredObjects(); !errors.Is(err, ErrArtifactsDisabled) || got != nil {
		t.Fatal(got, err)
	}
	artifactTestInstall(t, s)
	if got, err := s.ArtifactRequiredObjects(); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	artifactTestComplete(t, s, testActor())
	if got, err := s.ArtifactRequiredObjects(); err != nil || len(got) != 0 {
		t.Fatal("unconsumed upload is not a required pin", got, err)
	}
}
func TestArtifactRequiredObjectsPinsResultsManualAndIsolation(t *testing.T) {
	s, j, input := artifactRegressionWorkspace(t, true, true)
	result := artifactRegressionPublish(t, s, j)
	// A second queued consumer of input must not duplicate the returned object.
	info, err := s.RetentionInfo(testActor())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitWorkspace(testActor(), &info.Domain, "second-consumer", artifactInputTask(input), "remote", "test-binding", backend.RemoteOutput, 0); err != nil {
		t.Fatal(err)
	}
	artifactRegressionTransferManual(t, s, j)
	now := s.now().Add(48 * time.Hour)
	s.now = func() time.Time { return now }
	before := artifactDatabaseBytes(t, s)
	got, err := s.ArtifactRequiredObjects()
	if err != nil || len(got) != 2 {
		t.Fatal(got, err)
	}
	if got[0].Reference.ID >= got[1].Reference.ID {
		t.Fatal("inventory is not sorted")
	}
	want := map[string]ArtifactRef{input.ID: input, result.ID: result}
	for _, o := range got {
		if want[o.Reference.ID] != o.Reference || o.Manifest.Reference != o.Reference || !artifactFileValid(o.File) {
			t.Fatal("missing exact complete identity", o)
		}
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("query wrote source")
	}
	original := got[0]
	got[0].Reference.ID = "mutated"
	again, err := s.ArtifactRequiredObjects()
	if err != nil || again[0] != original {
		t.Fatal("returned DTO mutated Store", again, err)
	}
}
func ExampleArtifactObjectIdentity() {
	object := ArtifactObjectIdentity{Reference: ArtifactRef{ID: "0123456789abcdef0123456789abcdef", Size: 1024}}
	fmt.Println(object.Reference.ID, object.Reference.Size)
	// Output: 0123456789abcdef0123456789abcdef 1024
}
