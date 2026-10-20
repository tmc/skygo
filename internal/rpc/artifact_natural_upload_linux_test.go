//go:build linux

package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// This gate leaves real charged uploads incomplete for their original fixed
// hour. Only ordinary Control.Run maintenance may retire them and their credit.
func TestArtifactNaturalAbandonedUploadExpiry(t *testing.T) {
	if os.Getenv("SKYGO_ARTIFACT_NATURAL_UPLOAD") != "1" {
		t.Skip("set SKYGO_ARTIFACT_NATURAL_UPLOAD=1 for the real one-hour Linux gate")
	}
	if os.Getenv("SKYGO_ARTIFACT_TEST_ROOT") == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		parent := filepath.Join(home, "tmp", "skypilot", "20261020-artifact-natural-upload")
		if err := os.MkdirAll(parent, 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SKYGO_ARTIFACT_TEST_ROOT", parent)
	}
	disk := func(path string) {
		t.Helper()
		var stat syscall.Statfs_t
		if err := syscall.Statfs(path, &stat); err != nil {
			t.Fatal(err)
		}
		if stat.Bsize <= 0 || stat.Bavail > ^uint64(0)/uint64(stat.Bsize) {
			t.Fatal("invalid available disk capacity")
		}
		available := stat.Bavail * uint64(stat.Bsize)
		if available < 100<<30 {
			t.Fatalf("available disk below 100 GiB: %d bytes", available)
		}
	}
	disk(os.Getenv("SKYGO_ARTIFACT_TEST_ROOT"))
	f := artifactRPC(t, nil)
	f.remove = nil
	t.Logf("natural upload original fixture: %s", f.directory)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Minute)
	t.Cleanup(cancel)
	wait := func(duration time.Duration) {
		t.Helper()
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-timer.C:
		}
	}
	// The ordinary fixture credentials last only an hour. Issue and positively
	// authenticate a real two-hour Alice credential before creating any upload.
	tokenFile := filepath.Join(f.directory, "alice-natural-upload.token")
	issued, err := auth.Issue(f.authFile, "alice", tokenFile, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	f.tokens["alice"], f.ids["alice"] = strings.TrimSuffix(string(token), "\n"), issued.ID
	ready := time.Now().Add(20 * time.Second)
	for {
		if _, err := f.engine.Authenticate("Bearer " + f.tokens["alice"]); err == nil {
			break
		}
		if !time.Now().Before(ready) {
			t.Fatal("original auth engine did not accept the real two-hour credential")
		}
		wait(100 * time.Millisecond)
	}
	baseline, err := f.store.ArtifactReservations()
	if err != nil || len(baseline.Subjects) != 0 {
		t.Fatalf("initial reservation cohort=%v error=%v", baseline, err)
	}
	const declared = 16 << 20
	prefix := bytes.Repeat([]byte("incomplete upload\n"), 128)
	ids := make([]string, 0, 4)
	var earliest, latest time.Time
	for i := 0; i < 4; i++ {
		started := time.Now()
		upload, err := f.beginUpload(t, ctx, strings.Repeat("a", 64), declared)
		returned := time.Now()
		if err != nil || upload.Msg.Upload.Id == "" || upload.Msg.Upload.ExpiresAt == nil || upload.Msg.Upload.ExpiresAt.CheckValid() != nil {
			t.Fatalf("original upload %d=%v error=%v", i, upload, err)
		}
		u := upload.Msg.Upload
		expires := u.ExpiresAt.AsTime()
		if expires.Before(started.UTC().Add(time.Hour)) || expires.After(returned.UTC().Add(time.Hour)) {
			t.Fatalf("fixed hour differs: begin=%s return=%s expiry=%s", started.UTC(), returned.UTC(), expires)
		}
		for _, id := range ids {
			if id == u.Id {
				t.Fatal("duplicate original upload ID")
			}
		}
		ids = append(ids, u.Id)
		if earliest.IsZero() || expires.Before(earliest) {
			earliest = expires
		}
		if expires.After(latest) {
			latest = expires
		}
		written, err := f.writeUpload(t, ctx, u.Id, 0, prefix)
		if err != nil || written.Msg.Upload.Acknowledged != int64(len(prefix)) || written.Msg.Upload.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE || !written.Msg.Upload.ExpiresAt.AsTime().Equal(expires) {
			t.Fatalf("original staged prefix=%v error=%v", written, err)
		}
	}
	charged, err := f.store.ArtifactReservations()
	if err != nil || len(charged.Subjects) != 4 || charged.AllocatedBytes <= baseline.AllocatedBytes || charged.Files != baseline.Files+4 {
		t.Fatalf("charged reservation cohort=%v error=%v", charged, err)
	}
	for _, subject := range charged.Subjects {
		known := false
		for _, id := range ids {
			known = known || subject.ID == id
		}
		if !known || subject.Kind != "upload" || subject.File == (store.ArtifactFile{}) {
			t.Fatalf("unknown original charged subject: %+v", subject)
		}
	}
	active := func() {
		t.Helper()
		for _, id := range ids {
			status, err := f.client.GetArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.GetArtifactUploadRequest{Id: id}))
			f.awaitReturned(t, skyv1connect.ControlServiceGetArtifactUploadProcedure, 1)
			if err != nil || status.Msg.Upload.Id != id || status.Msg.Upload.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE || status.Msg.Upload.Acknowledged != int64(len(prefix)) {
				t.Fatalf("original active upload=%v error=%v", status, err)
			}
			info, err := os.Lstat(filepath.Join(f.rootName, id+".part"))
			if err != nil || !info.Mode().IsRegular() || info.Size() != int64(len(prefix)) {
				t.Fatalf("original staged prefix size differs: id=%s error=%v", id, err)
			}
		}
		current, err := f.store.ArtifactReservations()
		if err != nil || !reflect.DeepEqual(current, charged) {
			t.Fatalf("original charged cohort changed before expiry: %v %v", current, err)
		}
		rootBefore := artifactRootBytes(t, f.rootName)
		unexpected, err := f.beginUpload(t, ctx, strings.Repeat("b", 64), declared)
		var failure *connect.Error
		if unexpected != nil || !errors.As(err, &failure) || failure.Code() != connect.CodeResourceExhausted || failure.Message() != "artifact capacity reached" {
			t.Fatalf("four-upload pending quota not full: %v %v", unexpected, err)
		}
		if len(failure.Meta().Values(artifactBeginNotStartedHeader)) != 0 {
			t.Fatal("Store quota refusal carried Begin marker")
		}
		after, err := f.store.ArtifactReservations()
		if err != nil || !reflect.DeepEqual(after, current) || !reflect.DeepEqual(rootBefore, artifactRootBytes(t, f.rootName)) {
			t.Fatalf("quota refusal changed original reservations/root bytes: %v", err)
		}
	}
	active()
	// All original writes and their enclosing HTTP handlers have returned.
	// Status/read-only observations below never renew the fixed upload deadline.
	idle := time.Now()
	clock := func() time.Time {
		t.Helper()
		// Read-only capacity is a stop guard, never a closure/expiry receipt.
		disk(f.directory)
		now := time.Now()
		drift := now.UTC().Sub(idle.UTC()) - now.Sub(idle)
		if drift < -2*time.Second || drift > 2*time.Second {
			t.Fatalf("UTC clock changed relative to monotonic elapsed time: %s", drift)
		}
		return now
	}
	t.Logf("abandoned originals=%v UTC=%s first-expiry=%s last-expiry=%s", ids, idle.UTC(), earliest, latest)
	for clock().Sub(idle) < 55*time.Minute {
		wait(time.Minute)
	}
	before := clock()
	if !before.UTC().Before(earliest) {
		t.Fatal("could not observe the original predeadline charge")
	}
	active()
	t.Logf("all four original uploads still active/charged UTC=%s monotonic-idle=%s", before.UTC(), before.Sub(idle))
	// An extra two minutes avoids equating the boundary instant with a completed
	// maintenance pass. Both UTC and real monotonic elapsed time must qualify.
	for now := clock(); now.Sub(idle) < 62*time.Minute || now.UTC().Before(latest.Add(2*time.Minute)); now = clock() {
		wait(time.Minute)
	}
	for {
		now := clock()
		current, err := f.store.ArtifactReservations()
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(current, baseline) {
			for _, id := range ids {
				for _, suffix := range []string{".part", ".tar"} {
					if _, err := os.Lstat(filepath.Join(f.rootName, id+suffix)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("original upload name remains or observation failed: %s%s: %v", id, suffix, err)
					}
				}
				status, err := f.client.GetArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.GetArtifactUploadRequest{Id: id}))
				f.awaitReturned(t, skyv1connect.ControlServiceGetArtifactUploadProcedure, 1)
				if status != nil || connect.CodeOf(err) != connect.CodeNotFound {
					t.Fatalf("original expired upload status=%v error=%v", status, err)
				}
			}
			_, found, err := f.store.NextArtifactCleanup("")
			if err != nil || found {
				t.Fatalf("unfinished deletion receipt remains: found=%v error=%v", found, err)
			}
			t.Logf("ordinary worker retired all four originals and restored exact credit UTC=%s monotonic-idle=%s", now.UTC(), now.Sub(idle))
			break
		}
		wait(time.Second)
	}
	if !f.settle(t) {
		t.Fatal("original HTTP/Run/maintenance owners did not settle")
	}
	t.Log("original in-process HTTP and Control Run/Drain/Join settled; no explicit Cancel/Remove/Finalize or external Process.Wait claim")
}
