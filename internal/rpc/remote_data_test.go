package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func remoteTestReference(size int64) artifact.Ref {
	return artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: size}
}

func TestRemoteInputProjectionAndEncodedBound(t *testing.T) {
	data := make([]byte, 64<<10)
	for i := range data {
		data[i] = byte(i)
	}
	ref := remoteTestReference(remoteMaxSize)
	out, err := wireRemoteInput(ref, data, ref.Size-int64(len(data)), len(data))
	if err != nil || !out.Complete || out.NextOffset != ref.Size || !bytes.Equal(out.Data, data) {
		t.Fatalf("last bounded range: %+v %v", out, err)
	}
	for _, marshal := range []func(proto.Message) ([]byte, error){proto.Marshal, protojson.Marshal} {
		encoded, err := marshal(out)
		if err != nil || len(encoded) == 0 || len(encoded) >= 96<<10 || len(encoded) >= 1<<20 {
			t.Fatalf("fixed default codec bound: %d %v", len(encoded), err)
		}
	}
	for _, tc := range []struct {
		name   string
		offset int64
		limit  int
		data   []byte
	}{
		{"negative", -1, 1, []byte{1}},
		{"past-end", ref.Size + 1, 1, nil},
		{"overflow", math.MaxInt64, 1, nil},
		{"zero-limit", 0, 0, nil},
		{"over-limit", 0, 65537, nil},
		{"short", 0, 2, []byte{1}},
		{"long", ref.Size - 1, 2, []byte{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if value, err := wireRemoteInput(ref, tc.data, tc.offset, tc.limit); err == nil || value != nil {
				t.Fatalf("invalid range projected: %+v %v", value, err)
			}
		})
	}
	end, err := wireRemoteInput(ref, nil, ref.Size, 1)
	if err != nil || !end.Complete || len(end.Data) != 0 {
		t.Fatalf("wire EOF is byte-end only, not a Close receipt: %+v %v", end, err)
	}
}

func TestRemoteReleaseProjection(t *testing.T) {
	zero, one := int32(0), int32(1)
	for _, tc := range []struct {
		name  string
		value store.RemoteRelease
		valid bool
	}{
		{"wait", store.RemoteRelease{State: 1, Revision: 1}, true},
		{"abort", store.RemoteRelease{State: 3, Revision: 1}, true},
		{"collect", store.RemoteRelease{State: 2, Revision: 2, TaskContainerID: "original-main", TaskExitCode: &zero}, true},
		{"unknown", store.RemoteRelease{State: 4, Revision: 1}, false},
		{"zero-revision", store.RemoteRelease{State: 1}, false},
		{"abort-exit", store.RemoteRelease{State: 3, Revision: 1, TaskExitCode: &zero}, false},
		{"missing-main", store.RemoteRelease{State: 2, Revision: 1, TaskExitCode: &zero}, false},
		{"failed-main", store.RemoteRelease{State: 2, Revision: 1, TaskContainerID: "main", TaskExitCode: &one}, false},
		{"unsafe-main", store.RemoteRelease{State: 2, Revision: 1, TaskContainerID: "main\nsecret", TaskExitCode: &zero}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := wireRemoteRelease(tc.value)
			if (err == nil) != tc.valid {
				t.Fatalf("projection %+v: %+v %v", tc.value, out, err)
			}
			if tc.valid && tc.value.State == 2 {
				if out.TaskExitCode == nil || *out.TaskExitCode != 0 || out.TaskExitCode == tc.value.TaskExitCode {
					t.Fatal("lost optional zero exit or aliased native value")
				}
			}
		})
	}
}

func TestRemoteOutputProjectionRefusesUnissuedStates(t *testing.T) {
	ref := remoteTestReference(4)
	for _, state := range []string{"creating", "writable", "finalizing", "complete", "deleting", "future"} {
		u := store.ArtifactUpload{Reference: store.ArtifactRef{ID: ref.ID, SHA256: ref.SHA256, Size: 4}, Acknowledged: 4, State: state}
		begin, err := wireRemoteBegin(u, ref.SHA256, 4)
		validBegin := state == "writable" || state == "finalizing" || state == "complete"
		if (err == nil) != validBegin {
			t.Fatalf("begin %s: %+v %v", state, begin, err)
		}
		finish, err := wireRemoteFinish(u)
		validFinish := state == "finalizing" || state == "complete"
		if (err == nil) != validFinish {
			t.Fatalf("finish %s: %+v %v", state, finish, err)
		}
		u.Acknowledged = math.MaxUint64
		if _, err := wireRemoteBegin(u, ref.SHA256, 4); err == nil {
			t.Fatal("unsigned ack wrapped into wire")
		}
	}
	if _, err := wireRemoteWrite(10, 2, 3); err != nil {
		t.Fatalf("earlier acknowledged prefix replay narrowed: %v", err)
	}
	for _, tc := range []struct {
		ack    uint64
		offset int64
		size   int
	}{
		{math.MaxUint64, 0, 1}, {1, 1, 1}, {1, math.MaxInt64, 1}, {1, 0, 0},
	} {
		if _, err := wireRemoteWrite(tc.ack, tc.offset, tc.size); err == nil {
			t.Fatalf("invalid ack/range accepted: %+v", tc)
		}
	}
}

func TestRemoteErrorsRedactRecognizedWrappedCauses(t *testing.T) {
	for _, cause := range []error{auth.ErrUnauthenticated, auth.ErrPermissionDenied, auth.ErrUnavailable, control.ErrInvalid, control.ErrUnavailable, store.ErrArtifactConflict, context.Canceled, errors.New("unknown")} {
		err := remoteError(fmt.Errorf("credential-secret path/private %w", cause))
		if err == nil || strings.Contains(err.Error(), "credential-secret") || strings.Contains(err.Error(), "path/private") {
			t.Fatalf("cause leaked: %v", err)
		}
	}
	if connect.CodeOf(remoteError(store.ErrArtifactExpired)) != connect.CodeFailedPrecondition {
		t.Fatal("expiry became a retry/admission success")
	}
}

func TestRemoteCallContextCannotBeReusedOrChangeOperation(t *testing.T) {
	owner := &artifactResponse{call: new(control.ArtifactCall)}
	ctx := context.WithValue(context.Background(), remoteCallKey{}, remoteCallContext{owner, control.RemoteReadInput})
	if _, err := remoteCall(ctx, control.RemoteWriteOutput); err == nil || owner.used {
		t.Fatal("foreign method consumed the admitted operation")
	}
	if _, err := remoteCall(ctx, control.RemoteReadInput); err != nil {
		t.Fatal(err)
	}
	if _, err := remoteCall(ctx, control.RemoteReadInput); err == nil {
		t.Fatal("method reused context; this is bookkeeping only, not a real permit test")
	}
}
