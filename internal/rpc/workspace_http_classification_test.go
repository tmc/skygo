package rpc

import (
	"testing"

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
)

// This policy table is only a source-level routing/action check. The separate
// production-handler/generated-client oracles exercise actual HTTP ownership.
// All are UNRUN; none qualifies a physical socket/body or native owner.
func TestWorkspaceHTTPUsesExistingArtifactPolicy(t *testing.T) {
	for _, tt := range []struct {
		name      string
		procedure string
		owned     bool
		action    auth.Action
	}{
		{"begin_upload", skyv1connect.ControlServiceBeginArtifactUploadProcedure, true, auth.Mutate},
		{"get_upload", skyv1connect.ControlServiceGetArtifactUploadProcedure, true, auth.Read},
		{"write_chunk", skyv1connect.ControlServiceWriteArtifactChunkProcedure, true, auth.Mutate},
		{"finalize", skyv1connect.ControlServiceFinalizeArtifactUploadProcedure, true, auth.Mutate},
		{"cancel_upload", skyv1connect.ControlServiceCancelArtifactUploadProcedure, true, auth.Mutate},
		{"begin_download", skyv1connect.ControlServiceBeginArtifactDownloadProcedure, true, auth.Read},
		{"read_chunk", skyv1connect.ControlServiceReadArtifactChunkProcedure, true, auth.Read},
		{"close_download", skyv1connect.ControlServiceCloseArtifactDownloadProcedure, true, auth.Read},
		{"remove", skyv1connect.ControlServiceRemoveArtifactProcedure, true, auth.Mutate},
		{"workspace", skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, true, auth.Mutate},
		{"ordinary_submit", skyv1connect.ControlServiceSubmitJobProcedure, false, auth.Mutate},
		{"discovery", skyv1connect.ControlServiceGetServerInfoProcedure, false, auth.Mutate},
		{"unknown", "/unknown/Procedure", false, auth.Mutate},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := artifactProcedure(tt.procedure); got != tt.owned {
				t.Fatalf("owned=%v, want %v", got, tt.owned)
			}
			if got := artifactAction(tt.procedure); got != tt.action {
				t.Fatalf("action=%v, want %v", got, tt.action)
			}
		})
	}
}
