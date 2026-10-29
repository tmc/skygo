package kube

import (
	"strings"
	"testing"
)

// Source-only; NOT_RUN.

const testServiceProfiles = `version: 1
target: kube
revision: services-1
namespace_uid: ns-uid
service_account_uid: sa-uid
image_pull_secrets: [registry]
ceilings: {cpu_millis: 2000, memory_bytes: 4294967296}
limits: {cpu_millis: 8000, memory_bytes: 17179869184}
grants:
- principal: alice
  images: [` + testServiceImage + `]
`

func TestReadServiceConfig(t *testing.T) {
	cfg, err := readServiceConfig([]byte(testServiceProfiles))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Target() != "kube" || !cfg.granted("alice", testServiceImage) || cfg.granted("bob", testServiceImage) {
		t.Fatalf("config %+v", cfg)
	}
	if p := cfg.profile; p.namespaceUID != "ns-uid" || p.serviceAccountUID != "sa-uid" || p.cpuMillis != 2000 {
		t.Fatalf("profile %+v", p)
	}
}

func TestReadServiceConfigRefused(t *testing.T) {
	for _, tt := range []struct {
		name, old, new, want string
	}{
		{"version", "version: 1", "version: 2", "version"},
		{"quoted version", "version: 1", `version: "1"`, "service profiles"},
		{"local target", "target: kube", "target: local", "target"},
		{"no namespace uid", "namespace_uid: ns-uid\n", "", "uids are required"},
		{"no ceilings", "ceilings: {cpu_millis: 2000, memory_bytes: 4294967296}\n", "", "ceilings"},
		{"tag image", testServiceImage, "registry.example.com/team/web:latest", "digest pinned"},
		{"unknown field", "revision: services-1", "revision: services-1\nmounts: []", "unknown field"},
		{"duplicate key", "revision: services-1", "revision: services-1\nrevision: services-2", "duplicate"},
		{"key anchor", "grants:\n- principal: alice", "grants:\n- &a principal: alice", "invalid service profiles key"},
		{"value anchor", "grants:\n- principal: alice", "grants: &a\n- principal: alice", "anchors"},
		{"no grants", "grants:\n- principal: alice\n  images: [" + testServiceImage + "]\n", "grants: []\n", "grants"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			doc := strings.Replace(testServiceProfiles, tt.old, tt.new, 1)
			if doc == testServiceProfiles {
				t.Fatal("case does not change the document")
			}
			_, err := readServiceConfig([]byte(doc))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}
