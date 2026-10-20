//go:build linux

package rpc_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
)

// This opt-in test uses real elapsed time, Store, Files, generated HTTP clients
// and the original Run worker. It does not change Store.now or lease policy.
func TestArtifactNaturalIdleDownloadExpiry(t *testing.T) {
	if os.Getenv("SKYGO_ARTIFACT_NATURAL_IDLE") != "1" {
		t.Skip("set SKYGO_ARTIFACT_NATURAL_IDLE=1 for the real five-minute Linux gate")
	}
	if os.Getenv("SKYGO_ARTIFACT_TEST_ROOT") == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		parent := filepath.Join(home, "tmp", "skypilot", "20261020-artifact-natural-idle")
		if err := os.MkdirAll(parent, 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("SKYGO_ARTIFACT_TEST_ROOT", parent)
	}
	f := artifactRPC(t, nil)
	// Retire original owners normally, but retain this unique evidence namespace.
	// No path removal is substituted for original Close/Run/HTTP settlement.
	f.remove = nil
	t.Logf("natural idle original fixture: %s", f.directory)
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	t.Cleanup(cancel)
	data, digest := artifactTar(t)
	upload, err := f.beginUpload(t, ctx, digest, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		written, err := f.writeUpload(t, ctx, upload.Msg.Upload.Id, int64(offset), data[offset:end])
		if err != nil || written.Msg.Upload.Acknowledged != int64(end) {
			t.Fatalf("real input write=%v error=%v", written, err)
		}
		offset = end
	}
	sealed, err := f.finalizeUpload(t, ctx, upload.Msg.Upload.Id)
	if err != nil {
		t.Fatal(err)
	}
	ref := sealed.Msg.Reference
	old := make(map[string]bool, 32)
	var earliest, latest time.Time
	for i := 0; i < 32; i++ {
		begin := time.Now()
		opened, err := f.client.BeginArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactDownloadRequest{Reference: ref}))
		returned := time.Now()
		f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactDownloadProcedure, 1)
		if err != nil || opened.Msg.Id == "" || !proto.Equal(opened.Msg.Reference, ref) || opened.Msg.ExpiresAt == nil || opened.Msg.ExpiresAt.CheckValid() != nil {
			t.Fatalf("original download %d=%v error=%v", i, opened, err)
		}
		expires := opened.Msg.ExpiresAt.AsTime()
		if expires.Before(begin.UTC().Add(5*time.Minute)) || expires.After(returned.UTC().Add(5*time.Minute)) || old[opened.Msg.Id] {
			t.Fatalf("fixed five-minute deadline/identity differs: id=%s begin=%s return=%s expiry=%s", opened.Msg.Id, begin.UTC(), returned.UTC(), expires)
		}
		old[opened.Msg.Id] = true
		if earliest.IsZero() || expires.Before(earliest) {
			earliest = expires
		}
		if expires.After(latest) {
			latest = expires
		}
		chunk, err := f.client.ReadArtifactChunk(ctx, authorized(f.tokens["alice"], &v1.ReadArtifactChunkRequest{DownloadId: opened.Msg.Id, Limit: 1}))
		f.awaitReturned(t, skyv1connect.ControlServiceReadArtifactChunkProcedure, 1)
		if err != nil || chunk.Msg.Complete || chunk.Msg.NextOffset != 1 || !bytes.Equal(chunk.Msg.Data, data[:1]) {
			t.Fatalf("original retained reader %d=%v error=%v", i, chunk, err)
		}
	}
	// Every original ReadAt and enclosing ServeHTTP has actually returned.
	// No old lease is read or explicitly closed during the following idle wait.
	idle := time.Now()
	t.Logf("idle start UTC=%s first-expiry=%s last-expiry=%s original-readers=%d", idle.UTC().Format(time.RFC3339Nano), earliest.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano), len(old))
	full := func() {
		t.Helper()
		opened, err := f.client.BeginArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactDownloadRequest{Reference: ref}))
		f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactDownloadProcedure, 1)
		if opened != nil || connect.CodeOf(err) != connect.CodeResourceExhausted {
			t.Fatalf("32-slot accounting was not full: %v %v", opened, err)
		}
	}
	full()
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
	clock := func() time.Time {
		t.Helper()
		now := time.Now()
		drift := now.UTC().Sub(idle.UTC()) - now.Sub(idle)
		if drift < -2*time.Second || drift > 2*time.Second {
			t.Fatalf("UTC clock changed relative to monotonic elapsed time: %s", drift)
		}
		return now
	}
	wait(4*time.Minute - time.Since(idle))
	before := clock()
	if !before.UTC().Before(earliest) {
		t.Fatalf("could not observe predeadline charge: now=%s earliest=%s elapsed=%s", before.UTC(), earliest, before.Sub(idle))
	}
	full()
	t.Logf("predeadline still32 charged UTC=%s monotonic-idle=%s", before.UTC().Format(time.RFC3339Nano), before.Sub(idle))
	wait(5*time.Minute - time.Since(idle))
	for now := clock(); now.UTC().Before(latest); now = clock() {
		wait(time.Second)
	}
	now := clock()
	if now.Sub(idle) < 5*time.Minute || now.UTC().Before(latest) {
		t.Fatalf("natural idle deadline not reached: UTC=%s elapsed=%s latest=%s", now.UTC(), now.Sub(idle), latest)
	}
	t.Logf("natural deadline reached UTC=%s monotonic-idle=%s", now.UTC().Format(time.RFC3339Nano), now.Sub(idle))
	// New admissions are only a bounded public API accounting observation. They
	// cannot themselves prove a particular original descriptor was closed.
	fresh := make([]string, 0, 32)
	seen := make(map[string]bool, 32)
	for len(fresh) < 32 {
		opened, err := f.client.BeginArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactDownloadRequest{Reference: ref}))
		f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactDownloadProcedure, 1)
		if connect.CodeOf(err) == connect.CodeResourceExhausted {
			wait(time.Second)
			continue
		}
		if err != nil || !proto.Equal(opened.Msg.Reference, ref) || opened.Msg.Id == "" || old[opened.Msg.Id] || seen[opened.Msg.Id] {
			t.Fatalf("replacement slot %d=%v error=%v", len(fresh), opened, err)
		}
		seen[opened.Msg.Id] = true
		fresh = append(fresh, opened.Msg.Id)
	}
	full()
	t.Logf("all32 slots readmitted UTC=%s monotonic-idle=%s", time.Now().UTC().Format(time.RFC3339Nano), time.Since(idle))
	var read []byte
	for {
		chunk, err := f.client.ReadArtifactChunk(ctx, authorized(f.tokens["alice"], &v1.ReadArtifactChunkRequest{DownloadId: fresh[0], Offset: int64(len(read)), Limit: 64 << 10}))
		f.awaitReturned(t, skyv1connect.ControlServiceReadArtifactChunkProcedure, 1)
		if err != nil || chunk.Msg.NextOffset != int64(len(read)+len(chunk.Msg.Data)) {
			t.Fatalf("post-expiry bytes=%v error=%v", chunk, err)
		}
		read = append(read, chunk.Msg.Data...)
		if chunk.Msg.Complete {
			break
		}
	}
	if !bytes.Equal(read, data) {
		t.Fatal("natural lease expiry changed the original object bytes")
	}
	for _, id := range fresh {
		_, err := f.client.CloseArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.CloseArtifactDownloadRequest{DownloadId: id}))
		f.awaitReturned(t, skyv1connect.ControlServiceCloseArtifactDownloadProcedure, 1)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !f.settle(t) {
		t.Fatal("original HTTP/Run/maintenance owners did not settle")
	}
	t.Log("original in-process HTTP server and Control.Run/Join/Drain settled; no external daemon Process.Wait claim")
}
