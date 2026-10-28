package kube

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

const environmentTestImage = "registry.example.com/env/idle@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// environmentTestBackend returns a Backend from the real constructor with
// policy and no API client.
func environmentTestBackend(t *testing.T) *Backend {
	t.Helper()
	target, err := fromREST(TargetConfig{Name: "kube", Namespace: "workloads", Resources: ResourcePolicy{
		MaxCPUMillis: 4000, MaxMemoryBytes: 8 << 30, MaxAccelerators: 2,
		NodeSelectors: map[string][]string{"pool": {"cpu", "gpu"}},
		Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu",
			RequiredNodeSelector: map[string]string{"pool": "gpu"}}},
	}}, "owner", &rest.Config{Host: "https://127.0.0.1:6443"})
	if err != nil {
		t.Fatal(err)
	}
	return target.Backend.(*Backend)
}

func environmentTestInput() environmentPodInput {
	return environmentPodInput{
		id: "0123456789abcdef0123456789abcdef", principal: "alice", generation: 1,
		specHash: sha256.Sum256([]byte("environment spec")),
		task: backend.Task{Image: environmentTestImage,
			CPUMillis: 500, MemoryBytes: 256 << 20, CPULimitMillis: 1000, MemoryLimitBytes: 512 << 20},
	}
}

// environmentTestProfile returns a private profile value. Its UIDs are
// synthetic comparison fixtures, not Kubernetes allocations.
func environmentTestProfile() environmentPodProfile {
	return environmentPodProfile{
		revision: "profile-1", namespaceUID: "namespace-uid-1", principal: "alice",
		image: environmentTestImage, idleArgv: []string{"/usr/local/bin/idle", "--forever"},
		alias: "home", pvcName: "alice-home", pvcUID: "claim-uid-1", volumeName: "pv-alice-home",
		accessMode: corev1.ReadWriteOnce, mountPath: "/workspace",
		serviceAccountUID: "service-account-uid-1", pullSecrets: []string{"registry-b", "registry-a"},
	}
}

func TestEnvironmentPodInputCheck(t *testing.T) {
	name, err := environmentTestInput().check()
	if err != nil || name != "skygo-env-0123456789abcdef0123456789abcdef" {
		t.Fatalf("check = %q, %v", name, err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*environmentPodInput)
	}{
		{"short id", func(in *environmentPodInput) { in.id = "0123" }},
		{"uppercase id", func(in *environmentPodInput) { in.id = strings.ToUpper(in.id) }},
		{"empty principal", func(in *environmentPodInput) { in.principal = "" }},
		{"principal digit first", func(in *environmentPodInput) { in.principal = "1alice" }},
		{"principal uppercase", func(in *environmentPodInput) { in.principal = "Alice" }},
		{"principal underscore", func(in *environmentPodInput) { in.principal = "al_ice" }},
		{"principal too long", func(in *environmentPodInput) { in.principal = "a" + strings.Repeat("b", 64) }},
		{"generation 0", func(in *environmentPodInput) { in.generation = 0 }},
		{"generation 2", func(in *environmentPodInput) { in.generation = 2 }},
		{"zero spec hash", func(in *environmentPodInput) { in.specHash = [32]byte{} }},
		{"argv", func(in *environmentPodInput) { in.task.Argv = []string{"/bin/sh"} }},
		{"env", func(in *environmentPodInput) { in.task.Env = map[string]string{"A": "1"} }},
		{"directory", func(in *environmentPodInput) { in.task.Directory = "/workspace" }},
		{"target", func(in *environmentPodInput) { in.task.Target = "kube" }},
		{"target labels", func(in *environmentPodInput) { in.task.TargetLabels = map[string]string{"a": "b"} }},
		{"timeout", func(in *environmentPodInput) { in.task.TimeoutSeconds = 60 }},
		{"retry", func(in *environmentPodInput) { in.task.Retry = &backend.RetryPolicy{MaxAttempts: 2} }},
		{"workdir", func(in *environmentPodInput) { in.task.Workdir = &artifact.Ref{} }},
		{"setup", func(in *environmentPodInput) { in.task.Setup = []string{"true"} }},
		{"outputs", func(in *environmentPodInput) { in.task.Outputs = []string{"out"} }},
		{"data mounts", func(in *environmentPodInput) { in.task.DataMounts = []backend.DataMount{{}} }},
		{"no cpu", func(in *environmentPodInput) { in.task.CPUMillis = 0 }},
		{"no memory", func(in *environmentPodInput) { in.task.MemoryBytes = 0 }},
		{"no cpu limit", func(in *environmentPodInput) { in.task.CPULimitMillis = 0 }},
		{"no memory limit", func(in *environmentPodInput) { in.task.MemoryLimitBytes = 0 }},
		{"cpu over limit", func(in *environmentPodInput) { in.task.CPUMillis = 1001 }},
		{"memory over limit", func(in *environmentPodInput) { in.task.MemoryBytes = 513 << 20 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := environmentTestInput()
			tt.mutate(&in)
			if _, err := in.check(); err == nil {
				t.Fatal("check succeeded")
			}
		})
	}
}

func TestEnvironmentPodProfileCheck(t *testing.T) {
	if err := environmentTestProfile().check("alice"); err != nil {
		t.Fatal(err)
	}
	if err := environmentTestProfile().check("bob"); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("principal mismatch = %v", err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*environmentPodProfile)
	}{
		{"empty revision", func(p *environmentPodProfile) { p.revision = "" }},
		{"long revision", func(p *environmentPodProfile) { p.revision = strings.Repeat("r", 129) }},
		{"control revision", func(p *environmentPodProfile) { p.revision = "r\n" }},
		{"empty namespace uid", func(p *environmentPodProfile) { p.namespaceUID = "" }},
		{"long namespace uid", func(p *environmentPodProfile) { p.namespaceUID = types.UID(strings.Repeat("u", 129)) }},
		{"control namespace uid", func(p *environmentPodProfile) { p.namespaceUID = "u\x00" }},
		{"empty principal", func(p *environmentPodProfile) { p.principal = "" }},
		{"empty image", func(p *environmentPodProfile) { p.image = "" }},
		{"tagged image", func(p *environmentPodProfile) { p.image = "registry.example.com/env/idle:latest" }},
		{"no idle command", func(p *environmentPodProfile) { p.idleArgv = nil }},
		{"relative idle command", func(p *environmentPodProfile) { p.idleArgv = []string{"idle"} }},
		{"empty idle program", func(p *environmentPodProfile) { p.idleArgv = []string{""} }},
		{"too many idle args", func(p *environmentPodProfile) {
			p.idleArgv = append([]string{"/idle"}, make([]string, 64)...)
		}},
		{"idle args too large", func(p *environmentPodProfile) {
			p.idleArgv = []string{"/idle", strings.Repeat("x", 8188)}
		}},
		{"idle NUL", func(p *environmentPodProfile) { p.idleArgv = []string{"/idle", "a\x00b"} }},
		{"idle invalid UTF-8", func(p *environmentPodProfile) { p.idleArgv = []string{"/idle", "\xff"} }},
		{"empty alias", func(p *environmentPodProfile) { p.alias = "" }},
		{"alias subdomain", func(p *environmentPodProfile) { p.alias = "a.b" }},
		{"empty claim", func(p *environmentPodProfile) { p.pvcName = "" }},
		{"uppercase claim", func(p *environmentPodProfile) { p.pvcName = "Alice" }},
		{"empty volume", func(p *environmentPodProfile) { p.volumeName = "" }},
		{"empty claim uid", func(p *environmentPodProfile) { p.pvcUID = "" }},
		{"long claim uid", func(p *environmentPodProfile) { p.pvcUID = types.UID(strings.Repeat("u", 129)) }},
		{"no access mode", func(p *environmentPodProfile) { p.accessMode = "" }},
		{"read write many", func(p *environmentPodProfile) { p.accessMode = corev1.ReadWriteMany }},
		{"read only many", func(p *environmentPodProfile) { p.accessMode = corev1.ReadOnlyMany }},
		{"other mount path", func(p *environmentPodProfile) { p.mountPath = "/home" }},
		{"empty mount path", func(p *environmentPodProfile) { p.mountPath = "" }},
		{"empty service account uid", func(p *environmentPodProfile) { p.serviceAccountUID = "" }},
		{"long service account uid", func(p *environmentPodProfile) {
			p.serviceAccountUID = types.UID(strings.Repeat("u", 129))
		}},
		{"too many pull secrets", func(p *environmentPodProfile) {
			p.pullSecrets = nil
			for i := range 33 {
				p.pullSecrets = append(p.pullSecrets, fmt.Sprintf("registry-%d", i))
			}
		}},
		{"duplicate pull secret", func(p *environmentPodProfile) { p.pullSecrets = []string{"a", "b", "a"} }},
		{"empty pull secret", func(p *environmentPodProfile) { p.pullSecrets = []string{""} }},
		{"invalid pull secret", func(p *environmentPodProfile) { p.pullSecrets = []string{"Registry"} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := environmentTestProfile()
			tt.mutate(&p)
			if err := p.check("alice"); err == nil {
				t.Fatal("check succeeded")
			}
		})
	}
	// The fixed bounds are inclusive.
	p := environmentTestProfile()
	p.idleArgv = append([]string{"/idle"}, make([]string, 63)...)
	if err := p.check("alice"); err != nil {
		t.Fatalf("64 args: %v", err)
	}
	p.idleArgv = []string{"/idle", strings.Repeat("x", 8187)}
	if err := p.check("alice"); err != nil {
		t.Fatalf("8192 bytes: %v", err)
	}
	p = environmentTestProfile()
	p.accessMode = corev1.ReadWriteOncePod
	if err := p.check("alice"); err != nil {
		t.Fatalf("ReadWriteOncePod: %v", err)
	}
	p = environmentTestProfile()
	p.pullSecrets = nil
	if err := p.check("alice"); err != nil {
		t.Fatalf("no pull secrets: %v", err)
	}
	for i := range 32 {
		p.pullSecrets = append(p.pullSecrets, fmt.Sprintf("registry-%d", i))
	}
	if err := p.check("alice"); err != nil {
		t.Fatalf("32 pull secrets: %v", err)
	}
}
