package kube

import (
	"fmt"
	"testing"
)

func TestQualificationEnvironmentComparison(t *testing.T) {
	r, _, _, _ := qualificationCoverageFixture(t)
	// Matching model facts exercise comparison only, never API observation.
	if err := compareQualificationEnvironment(r, cloneQualificationRecord(r).Deployment); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*qualificationDeployment)
	}{
		{"namespace-uid", func(e *qualificationDeployment) { e.NamespaceUID = "replacement" }},
		{"namespace", func(e *qualificationDeployment) { e.Namespace = "different" }},
		{"api", func(e *qualificationDeployment) { e.APIServer = "https://different.example.com" }},
		{"ca", func(e *qualificationDeployment) { e.CASHA256 = "" }},
		{"server-version", func(e *qualificationDeployment) { e.KubernetesVersion = "v1.33.0" }},
		{"pool", func(e *qualificationDeployment) { e.Pool.Value = "other" }},
		{"policy", func(e *qualificationDeployment) { e.PolicyRevision = "next" }},
		{"uid", func(e *qualificationDeployment) { e.Nodes[0].UID = "replacement" }},
		{"kubelet", func(e *qualificationDeployment) { e.Nodes[0].Kubelet = "next" }},
		{"runtime", func(e *qualificationDeployment) { e.Nodes[0].Runtime = "next" }},
		{"kernel", func(e *qualificationDeployment) { e.Nodes[0].Kernel = "next" }},
		{"os", func(e *qualificationDeployment) { e.Nodes[0].OS = "other" }},
		{"arch", func(e *qualificationDeployment) { e.Nodes[0].Architecture = "amd64" }},
		{"zero-nodes", func(e *qualificationDeployment) { e.Nodes = nil }},
		{"extra-node", func(e *qualificationDeployment) {
			n := e.Nodes[0]
			n.Name = "node-2"
			n.UID = "another"
			e.Nodes = append(e.Nodes, n)
		}},
		{"over-limit", func(e *qualificationDeployment) {
			n := e.Nodes[0]
			e.Nodes = nil
			for i := 0; i < 33; i++ {
				x := n
				x.Name = fmt.Sprintf("node-%02d", i)
				x.UID = x.Name
				e.Nodes = append(e.Nodes, x)
			}
		}},
		{"pvc", func(e *qualificationDeployment) { e.Storage[0].PVCUID = "replacement" }},
		{"storage-class", func(e *qualificationDeployment) { e.Storage[0].StorageClass = "other" }},
		{"driver", func(e *qualificationDeployment) { e.Storage[0].Driver = "other" }},
		{"storage-policy", func(e *qualificationDeployment) { e.Storage[0].PolicyRevision = "next" }},
		{"missing-storage", func(e *qualificationDeployment) { e.Storage = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := cloneQualificationRecord(r).Deployment
			test.change(&observed)
			if compareQualificationEnvironment(r, observed) == nil {
				t.Fatal("drift accepted")
			}
		})
	}
	if compareQualificationEnvironment(nil, r.Deployment) == nil {
		t.Fatal("nil record accepted")
	}
}
