package store

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
)

// Exercise real Store acceptance/GC transactions, not transaction callbacks or
// seeded rows. Root/file/archive handoffs and the expiry clock are MODELED;
// this test neither unlinks files nor fabricates a physical deletion receipt.
func TestArtifactSubmitWorkspaceRacesDeletionMark(t *testing.T) {
	for iteration := 0; iteration < 6; iteration++ {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			s, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			engine := auth.NewLegacy("")
			actor, err := engine.Authenticate("")
			if err != nil {
				t.Fatal(err)
			}
			// The public installer preserves the real current schema/fence. Do
			// not use artifactTestInstall's direct initializer/older floor.
			installation := ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
			if _, err := s.InstallArtifacts(installation); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
			s.now = func() time.Time { return now }
			upload, err := s.BeginArtifactUpload(actor, strings.Repeat("b", 64), 1)
			if err != nil {
				t.Fatal(err)
			}
			file := ArtifactFile{Device: 17, Inode: uint64(100 + iteration)}
			upload, err = s.ArtifactPartCreated(actor, ArtifactCursor{upload.Reference.ID, upload.Revision}, file)
			if err != nil {
				t.Fatal(err)
			}
			upload, err = s.ArtifactChunkSynced(actor, ArtifactCursor{upload.Reference.ID, upload.Revision}, file, 0, 1, strings.Repeat("c", 64))
			if err != nil {
				t.Fatal(err)
			}
			upload, err = s.BeginArtifactFinalize(actor, ArtifactCursor{upload.Reference.ID, upload.Revision})
			if err != nil {
				t.Fatal(err)
			}
			ref, err := s.CompleteArtifactUpload(actor, ArtifactCursor{upload.Reference.ID, upload.Revision}, upload.Reference, file, ArtifactManifest{Reference: upload.Reference, ExpandedBytes: 1, RegularFiles: 1})
			if err != nil {
				t.Fatal(err)
			}
			task := backend.Task{Argv: []string{"true"}, Workdir: &artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)}}
			before, err := s.ArtifactReservations()
			if err != nil {
				t.Fatal(err)
			}
			quotaBefore, err := s.RetentionInfo(actor)
			if err != nil {
				t.Fatal(err)
			}
			if quotaBefore.Enabled {
				t.Fatal("race requires the legacy pre-transaction clock seam")
			}
			if cleanup, found, err := s.NextArtifactCleanup(""); err != nil || found {
				t.Fatal("fresh grant already has deletion authority", cleanup, found, err)
			}

			// DeleteAfter is at least grant expiry, so a fixed fresh-grant
			// clock could never authorize a competing MARK. Legacy workspace
			// submit samples its acceptance clock BEFORE acquiring the writer.
			// Stop at that actual pre-transaction seam, then release both
			// contenders with MARK's clock exactly at expiry. No clock wait
			// occurs in a transaction, and neither writer's order is forced.
			expires := now.Add(24 * time.Hour)
			captured, start := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			s.now = func() time.Time {
				if first.CompareAndSwap(false, true) {
					close(captured)
					<-start
					return expires.Add(-time.Nanosecond)
				}
				return expires
			}
			type submission struct {
				job backend.Job
				err error
			}
			type deletion struct {
				operation string
				err       error
			}
			submitted := make(chan submission, 1)
			marked := make(chan deletion, 1)
			var workers sync.WaitGroup
			workers.Add(1)
			go func() {
				defer workers.Done()
				job, err := s.SubmitWorkspace(actor, nil, "race", task, "remote", "test-binding", backend.RemoteOutput, 0)
				submitted <- submission{job, err}
			}()
			select {
			case <-captured:
			case early := <-submitted:
				workers.Wait()
				t.Fatal("submit returned before its acceptance-clock barrier", early.err)
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				operation, err := s.RemoveArtifact(actor, ref)
				marked <- deletion{operation, err}
			}()
			close(start)
			workers.Wait() // Both original function returns precede every oracle/Close.
			accepted, removed := <-submitted, <-marked
			s.now = func() time.Time { return expires }
			if (accepted.err == nil) == (removed.err == nil) {
				t.Fatalf("expected one serializable winner: submit=%v mark=%v", accepted.err, removed.err)
			}
			after, err := s.ArtifactReservations()
			if err != nil {
				t.Fatal(err)
			}
			// Neither logical acceptance nor MARK unlinks or credits the object.
			if !reflect.DeepEqual(after, before) {
				t.Fatal("race changed original physical reservation cohort", before, after)
			}
			quotaAfter, err := s.RetentionInfo(actor)
			if err != nil {
				t.Fatal(err)
			}
			cleanup, found, err := s.NextArtifactCleanup("")
			if err != nil {
				t.Fatal(err)
			}
			jobs, err := s.List(0, ^uint64(0), 10)
			if err != nil {
				t.Fatal(err)
			}
			if accepted.err == nil {
				if !errors.Is(removed.err, ErrArtifactConflict) || found || len(jobs) != 1 || jobs[0].ID != accepted.job.ID || accepted.job.Sequence != 1 {
					t.Fatal("submit winner lost pin or authorized competing deletion", removed.err, cleanup, jobs)
				}
				for _, usage := range quotaAfter.Usage {
					if (usage.Domain == JobsQuota || usage.Domain == RequestRecordsQuota) && usage.Used != 1 {
						t.Fatal("acceptance quota was not charged exactly once", usage)
					}
				}
				job, start, err := s.BeginAttempt(accepted.job.ID, accepted.job.CurrentAttemptID(), 0, expires)
				if err != nil || !start {
					t.Fatal("durable pin could not begin after grant expiry", start, err)
				}
				workspace, err := s.GetWorkspaceReservation(job.ID, job.CurrentAttemptID())
				if err != nil || workspace.InputRef != ref || workspace.InputFile != file {
					t.Fatal("workspace lost exact accepted input", workspace, err)
				}
				quotaBegun, err := s.RetentionInfo(actor)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.RemoveArtifact(actor, ref); !errors.Is(err, ErrArtifactConflict) {
					t.Fatal("workspace did not protect input", err)
				}
				again, err := s.SubmitWorkspace(actor, nil, "race", task, "", "", backend.LocalOutput, ^uint64(0))
				if err != nil || again.ID != job.ID || again.Sequence != job.Sequence {
					t.Fatal("exact replay rechecked current route or accepted twice", again, err)
				}
				quotaReplay, err := s.RetentionInfo(actor)
				if err != nil || !reflect.DeepEqual(quotaReplay.Usage, quotaBegun.Usage) {
					t.Fatal("replay changed quota", quotaReplay, err)
				}
			} else {
				if !errors.Is(accepted.err, ErrNotFound) || !found || cleanup.Delete == nil || cleanup.Workspace != nil || cleanup.Delete.ID != removed.operation || cleanup.Delete.Kind != "object" || cleanup.Delete.RootID != installation.ID || cleanup.Delete.Revision == 0 || cleanup.Delete.Digest == ([32]byte{}) || cleanup.Delete.Ref != ref || cleanup.Delete.File != file || len(jobs) != 0 {
					t.Fatal("MARK winner leaked acceptance or lost exact authorized intent", accepted.err, cleanup, jobs)
				}
				if !reflect.DeepEqual(quotaAfter.Usage, quotaBefore.Usage) {
					t.Fatal("failed submit leaked quota", quotaBefore.Usage, quotaAfter.Usage)
				}
				for _, request := range []string{"race", "fresh"} {
					if _, found, err := s.Lookup(actor, request, task); err != nil || found {
						t.Fatal("deletion winner retained an acceptance", request, found, err)
					}
					if _, err := s.SubmitWorkspace(actor, nil, request, task, "remote", "test-binding", backend.RemoteOutput, 0); !errors.Is(err, ErrNotFound) {
						t.Fatal("deleted input accepted on replay/new request", request, err)
					}
				}
				final, err := s.ArtifactReservations()
				if err != nil || !reflect.DeepEqual(final, after) {
					t.Fatal("refusal changed charged object/workspace cohort", final, err)
				}
				quotaFinal, err := s.RetentionInfo(actor)
				if err != nil || !reflect.DeepEqual(quotaFinal.Usage, quotaAfter.Usage) {
					t.Fatal("repeated refusal changed quota", quotaFinal, err)
				}
			}
		})
	}
}
