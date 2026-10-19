package rpc_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
)

func TestGetServerInfo(t *testing.T) {
	f := setup(t, "discovery-token", false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, token := range []string{"", "wrong"} {
		req := connect.NewRequest(&v1.GetServerInfoRequest{})
		req.Header().Set("Authorization", "Bearer "+token)
		_, err := f.client.GetServerInfo(ctx, req)
		code(t, err, connect.CodeUnauthenticated)
	}
	req := connect.NewRequest(&v1.GetServerInfoRequest{})
	req.Header().Set("Authorization", "Bearer discovery-token")
	info, err := f.client.GetServerInfo(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	want := &v1.GetServerInfoResponse{Targets: []*v1.Target{{Name: "local", Kind: "local", Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1, v1.Capability_CAPABILITY_TARGET_LABELS_V1}}}}
	if !proto.Equal(info.Msg, want) {
		t.Fatalf("info = %v; want %v", info.Msg, want)
	}
	list := connect.NewRequest(&v1.ListTargetsRequest{})
	list.Header().Set("Authorization", "Bearer discovery-token")
	targets, err := f.client.ListTargets(ctx, list)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets.Msg.Targets) != 1 || !proto.Equal(targets.Msg.Targets[0], want.Targets[0]) {
		t.Fatalf("target advertisements disagree: %v", targets.Msg)
	}
	jobs, _, err := f.control.List(f.actor, 100, "")
	if err != nil || len(jobs) != 0 {
		t.Fatalf("discovery changed jobs: %v, %v", jobs, err)
	}
}
