package rpc

import (
	"fmt"
	"reflect"
	"strings"

	"connectrpc.com/connect"
	"slices"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// This value-only optional-interface fixture is never invoked and qualifies no
// native backend. Projection still requires explicit configured support flags.
type remoteCapabilityShape struct {
	backend.Backend
	backend.DataBackend
}

func TestDataTaskConversionPreservesOrderFalseAndCopies(t *testing.T) {
	input := &v1.Task{Argv: []string{"run"}, Env: map[string]string{"a": "b"}, Setup: []string{"setup"}, Outputs: []string{"result"}, DataMounts: []*v1.DataMount{{Reference: "first", Destination: "/data/first", ReadOnly: false}, {Reference: "second", Destination: "/data/second", ReadOnly: true}}}
	value := task(input)
	out := wireTask(value)
	if !reflect.DeepEqual(out.DataMounts, input.DataMounts) || value.DataMounts[0].ReadOnly || !value.DataMounts[1].ReadOnly {
		t.Fatal("mount order/explicit false lost")
	}
	input.DataMounts[0].Reference = "changed"
	input.Env["a"] = "changed"
	input.Setup[0] = "changed"
	out.DataMounts[1].Destination = "/changed"
	if value.DataMounts[0].Reference != "first" || value.DataMounts[1].Destination != "/data/second" || value.Env["a"] != "b" || value.Setup[0] != "setup" {
		t.Fatal("conversion retained request/output aliases")
	}
	if !hasWorkspaceTask(input) || validTaskUTF8(input) || !validDataTaskUTF8(input) {
		t.Fatal("legacy adapter silently accepted new mounts")
	}
}

func TestDataTaskUTF8IncludesAllNewFields(t *testing.T) {
	for _, change := range []func(*v1.Task){
		func(v *v1.Task) { v.Setup = []string{"\xff"} },
		func(v *v1.Task) { v.Outputs = []string{"\xff"} },
		func(v *v1.Task) { v.Workdir = &v1.ArtifactRef{Id: "\xff"} },
		func(v *v1.Task) { v.DataMounts = []*v1.DataMount{nil} },
		func(v *v1.Task) { v.DataMounts = []*v1.DataMount{{Reference: "\xff"}} },
		func(v *v1.Task) { v.DataMounts = []*v1.DataMount{{Destination: "\xff"}} },
	} {
		value := &v1.Task{Argv: []string{"run"}}
		change(value)
		if validDataTaskUTF8(value) {
			t.Fatalf("malformed task accepted: %+v", value)
		}
	}
}

func TestDataTargetFlagsDoNotFollowPolicy(t *testing.T) {
	policy := backend.DefaultDataPolicy()
	for _, tc := range []struct {
		name                          string
		remote, mounts, retry, finite bool
		want                          []v1.Capability
	}{
		{"policy-only", false, false, false, false, nil},
		{"orphan-mounts", false, true, false, false, nil},
		{"remote", true, false, false, false, []v1.Capability{v1.Capability_CAPABILITY_REMOTE_DATA_V1}},
		{"mounts", true, true, false, false, []v1.Capability{v1.Capability_CAPABILITY_REMOTE_DATA_V1, v1.Capability_CAPABILITY_DATA_MOUNTS_V1}},
		{"retry-without-finite", true, false, true, false, []v1.Capability{v1.Capability_CAPABILITY_REMOTE_DATA_V1}},
		{"retry-qualified", true, false, true, true, []v1.Capability{v1.Capability_CAPABILITY_REMOTE_DATA_V1, v1.Capability_CAPABILITY_REMOTE_DATA_RETRY_V1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := backend.Target{Backend: remoteCapabilityShape{}, DataPolicy: &policy, SupportsRemoteData: tc.remote, SupportsDataMounts: tc.mounts, SupportsRemoteDataRetry: tc.retry, SupportsRetry: tc.finite}
			out := &v1.Target{}
			addRemoteTarget(target, out)
			if !slices.Equal(out.Capabilities, tc.want) || (out.DataPolicy != nil) != tc.remote {
				t.Fatalf("projection: %+v want %v", out, tc.want)
			}
			if out.DataPolicy != nil {
				out.DataPolicy.StageSeconds++
				if policy.StageSeconds != backend.DefaultDataPolicy().StageSeconds {
					t.Fatal("policy projection aliased configuration")
				}
			}
		})
	}
	invalid := policy
	invalid.StageSeconds++
	out := &v1.Target{}
	addRemoteTarget(backend.Target{Backend: remoteCapabilityShape{}, SupportsRemoteData: true, DataPolicy: &invalid}, out)
	if len(out.Capabilities) != 0 || out.DataPolicy != nil {
		t.Fatal("unsupported policy advertised")
	}
	out = &v1.Target{}
	addRemoteTarget(backend.Target{SupportsRemoteData: true, DataPolicy: &policy}, out)
	if len(out.Capabilities) != 0 {
		t.Fatal("missing actual optional backend advertised")
	}
}

func TestDataErrorsPreservePublicCategoriesWithoutCauses(t *testing.T) {
	for _, tc := range []struct {
		cause error
		code  connect.Code
	}{
		{store.ErrConflict, connect.CodeAlreadyExists},
		{store.ErrNotFound, connect.CodeNotFound},
		{&store.ExpiryError{Reason: store.RequestDomainRequired}, connect.CodeFailedPrecondition},
	} {
		err := dataError(fmt.Errorf("private-route-secret: %w", tc.cause))
		if connect.CodeOf(err) != tc.code || strings.Contains(err.Error(), "private-route-secret") {
			t.Fatalf("public category/redaction: %v", err)
		}
	}
}
