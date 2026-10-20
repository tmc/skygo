package rpc

import (
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

func TestArtifactWireBounds(t *testing.T) {
	for _, size := range []uint64{0, (32 << 20) + 1, ^uint64(0)} {
		_, err := wireArtifactUpload(store.ArtifactUpload{Reference: store.ArtifactRef{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: size}, State: "writable", ExpiresAt: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)})
		if !errors.Is(err, control.ErrUnavailable) {
			t.Fatalf("size %d: %v", size, err)
		}
	}
	base := store.ArtifactUpload{Reference: store.ArtifactRef{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024}, State: "writable", ExpiresAt: time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)}
	for _, tt := range []struct {
		name   string
		change func(*store.ArtifactUpload)
	}{
		{"ack", func(u *store.ArtifactUpload) { u.Acknowledged = 1025 }},
		{"state", func(u *store.ArtifactUpload) { u.State = "unknown" }},
		{"zero_time", func(u *store.ArtifactUpload) { u.ExpiresAt = time.Time{} }},
		{"wire_time", func(u *store.ArtifactUpload) { u.ExpiresAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			u := base
			tt.change(&u)
			if _, err := wireArtifactUpload(u); !errors.Is(err, control.ErrUnavailable) {
				t.Fatal(err)
			}
		})
	}
	out, err := wireArtifactUpload(base)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reference.Id != base.Reference.ID || out.Acknowledged != 0 || out.State != v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE {
		t.Fatal(out)
	}
	if _, err := artifactReference(nil); !errors.Is(err, control.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestArtifactErrorsDoNotExposePhysicalIdentity(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code connect.Code
	}{
		{&store.ArtifactCapacityError{Domain: "pending", Used: 3, Limit: 4}, connect.CodeResourceExhausted},
		{store.ErrArtifactConflict, connect.CodeFailedPrecondition},
		{store.ErrArtifactExpired, connect.CodeFailedPrecondition},
		{control.ErrArtifactBusy, connect.CodeResourceExhausted},
		{store.ErrNotFound, connect.CodeNotFound},
		{errors.New("secret path /owned/root inode123"), connect.CodeInternal},
	} {
		err := artifactError(tt.err)
		if connect.CodeOf(err) != tt.code {
			t.Fatalf("%v: %v", tt.err, err)
		}
		if strings.Contains(err.Error(), "/owned") || strings.Contains(err.Error(), "inode123") {
			t.Fatal(err)
		}
	}
}

func TestWorkspaceTaskConversionCopiesWithoutRewriting(t *testing.T) {
	input := &v1.Task{Argv: []string{"original"}, Workdir: &v1.ArtifactRef{Id: strings.Repeat("a", 32), Sha256: strings.Repeat("b", 64), Size: 1024}, Setup: []string{"setup", ""}, Outputs: []string{"logs/output"}}
	model := task(input)
	input.Workdir.Id = strings.Repeat("c", 32)
	input.Setup[0] = "changed"
	input.Outputs[0] = "changed"
	if model.Workdir.ID != strings.Repeat("a", 32) || model.Setup[0] != "setup" || model.Setup[1] != "" || model.Outputs[0] != "logs/output" {
		t.Fatal("input aliases model")
	}
	out := wireTask(model)
	out.Workdir.Sha256 = strings.Repeat("d", 64)
	out.Setup[0] = "changed"
	out.Outputs[0] = "changed"
	if model.Workdir.SHA256 != strings.Repeat("b", 64) || model.Setup[0] != "setup" || model.Outputs[0] != "logs/output" {
		t.Fatal("output aliases model")
	}
	if hasWorkspaceTask(&v1.Task{}) || task(&v1.Task{}).Workdir != nil || wireTask(task(&v1.Task{})).Workdir != nil {
		t.Fatal("default workspace changed")
	}
}
