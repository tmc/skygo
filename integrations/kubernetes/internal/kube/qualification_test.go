package kube

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"k8s.io/client-go/rest"
)

// Model inputs for pure comparisons only: no target is qualified, no Kubernetes
// client exists, and these receipt names do not establish an actual native run.
func qualificationCoverageFixture(t *testing.T) (*qualificationRecord, TargetConfig, *rest.Config, qualificationImplementation) {
	t.Helper()
	r := qualificationFixture()
	d := testDataConfig()
	r.Implementation.HelperImage, r.Implementation.HelperSHA256 = d.HelperImage, d.HelperSHA256
	r.Images[0].Image = d.TaskImages[0]
	r.Images[0].FixedPathsSHA256 = qualificationPathsID()
	r.Images[0].Mounts = []qualificationMount{{Reference: "creds", Destination: "/mnt/skygo/creds", ReadOnly: true}, {Reference: "datasets", Destination: "/mnt/skygo/data"}, {Reference: "datasets", Destination: "/mnt/skygo/data", ReadOnly: true}}
	r.Deployment.Storage[0].Reference = "datasets"
	r.Evidence.Cases = nil
	for _, name := range []string{"fixture-only-not-native", "input-output-digests", "long-task-renewal", "main-failure", "mount-only", "normal-retirement", "paths-security", "pvc-persistence", "secret-missing-key", "secret-projection", "setup-before-main", "setup-no-launch", "stage-no-launch", "worker-tls-credentials"} {
		r.Evidence.Cases = append(r.Evidence.Cases, qualificationCase{Name: name, ReceiptSHA256: strings.Repeat("a", 64)})
	}
	target := TargetConfig{Name: "qualified-shape-fixture", Namespace: r.Deployment.Namespace, InCluster: true, Data: d, Resources: ResourcePolicy{NodeSelectors: map[string][]string{r.Deployment.Pool.Key: {r.Deployment.Pool.Value}}}}
	config := &rest.Config{Host: r.Deployment.APIServer, TLSClientConfig: rest.TLSClientConfig{CAData: []byte(testCA())}}
	refreshQualificationTarget(t, r, target, config)
	return r, target, config, r.Implementation
}

func refreshQualificationTarget(t *testing.T, r *qualificationRecord, target TargetConfig, config *rest.Config) {
	t.Helper()
	d, err := newDataProfile(target.Data)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bindingJSON(target, config, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.TargetSHA256 = fmt.Sprintf("%x", sha256.Sum256(raw))
	r.Deployment.CASHA256 = fmt.Sprintf("%x", sha256.Sum256(config.CAData))
}

func qualificationCoverageTask(r *qualificationRecord) backend.Task {
	return backend.Task{Image: r.Images[0].Image, Argv: []string{"program"}, NodeSelector: map[string]string{r.Deployment.Pool.Key: r.Deployment.Pool.Value}, DataMounts: []backend.DataMount{{Reference: "datasets", Destination: "/mnt/skygo/data", ReadOnly: true}, {Reference: "creds", Destination: "/mnt/skygo/creds", ReadOnly: true}}}
}

func TestQualificationCoverageSnapshot(t *testing.T) {
	r, target, transport, actual := qualificationCoverageFixture(t)
	snapshot, err := validateQualificationCoverage(r, target, transport, actual)
	if err != nil {
		t.Fatal(err)
	}
	id, err := qualificationRecordID(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	r.Deployment.Nodes[0].UID = "changed"
	r.Deployment.Storage[0].PVCUID = "changed"
	r.Images[0].Profiles[0] = "changed"
	r.Images[0].Mounts[0].Reference = "changed"
	r.Images[0].Directories[0] = "/changed"
	r.Evidence.Cases[0].Name = "changed"
	after, err := qualificationRecordID(snapshot)
	if err != nil || id != after {
		t.Fatalf("snapshot alias: %v", err)
	}
}

func TestQualificationCoverageRecordDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*qualificationRecord, *TargetConfig, *rest.Config, *qualificationImplementation)
	}{
		{"executable", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, a *qualificationImplementation) {
			a.ExecutableSHA256 = strings.Repeat("b", 64)
		}},
		{"build", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, a *qualificationImplementation) {
			a.BuildReceiptSHA256 = strings.Repeat("b", 64)
		}},
		{"helper", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, a *qualificationImplementation) {
			a.HelperSHA256 = strings.Repeat("b", 64)
		}},
		{"target", func(_ *qualificationRecord, t *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			t.Labels = map[string]string{"changed": "binding"}
		}},
		{"ca", func(_ *qualificationRecord, _ *TargetConfig, c *rest.Config, _ *qualificationImplementation) {
			c.CAData = []byte("different")
		}},
		{"unfrozen-ca", func(_ *qualificationRecord, _ *TargetConfig, c *rest.Config, _ *qualificationImplementation) {
			c.CAFile = "not-read"
		}},
		{"insecure", func(_ *qualificationRecord, _ *TargetConfig, c *rest.Config, _ *qualificationImplementation) {
			c.Insecure = true
		}},
		{"pool-policy", func(_ *qualificationRecord, t *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			t.Resources.NodeSelectors = nil
		}},
		{"node-platform", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Deployment.Nodes[0].Architecture = "amd64"
		}},
		{"image-platform", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Images[0].Architecture = "amd64"
		}},
		{"image", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Images[0].Image = "registry.example.com/other@sha256:" + strings.Repeat("b", 64)
		}},
		{"layout", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Images[0].FixedPathsSHA256 = strings.Repeat("b", 64)
		}},
		{"no-launch", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Evidence.Cases = slices.DeleteFunc(r.Evidence.Cases, func(c qualificationCase) bool { return c.Name == "setup-no-launch" })
		}},
		{"storage", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Deployment.Storage = nil
		}},
		{"alias", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Images[0].Mounts[0].Reference = "absent"
		}},
		{"secret-write", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Images[0].Mounts[0].ReadOnly = false
		}},
		{"destination", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Images[0].Mounts[0].Destination = "/mnt/skygo/other"
		}},
		{"protected-cwd", func(r *qualificationRecord, _ *TargetConfig, _ *rest.Config, _ *qualificationImplementation) {
			r.Images[0].Directories = []string{"/run"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, target, transport, actual := qualificationCoverageFixture(t)
			test.change(r, &target, transport, &actual)
			if got, err := validateQualificationCoverage(r, target, transport, actual); err == nil || got != nil {
				t.Fatal("drift accepted")
			}
		})
	}
}

func TestQualificationTaskProfilesAndPrimitives(t *testing.T) {
	r, target, transport, actual := qualificationCoverageFixture(t)
	r, err := validateQualificationCoverage(r, target, transport, actual)
	if err != nil {
		t.Fatal(err)
	}
	d, err := newDataProfile(target.Data)
	if err != nil {
		t.Fatal(err)
	}
	base := qualificationCoverageTask(r)
	if err := checkQualificationTask(r, d, target.Resources, base); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []struct{ setup, output bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		task := backend.CloneTask(base)
		task.Workdir = &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1}
		if profile.setup {
			task.Setup = []string{"setup"}
		}
		if profile.output {
			task.Outputs = []string{"result"}
		}
		if err := checkQualificationTask(r, d, target.Resources, task); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		name   string
		change func(*backend.Task)
	}{
		{"pool-missing", func(t *backend.Task) { t.NodeSelector = nil }},
		{"pool-changed", func(t *backend.Task) { t.NodeSelector[r.Deployment.Pool.Key] = "other" }},
		{"cwd", func(t *backend.Task) { t.Directory = "/uncovered" }},
		{"image", func(t *backend.Task) { t.Image = "other" }},
		{"alias", func(t *backend.Task) { t.DataMounts[0].Reference = "other" }},
		{"mode", func(t *backend.Task) { t.DataMounts[1].ReadOnly = false }},
		{"destination", func(t *backend.Task) { t.DataMounts[0].Destination = "/mnt/skygo/other" }},
		{"overlap", func(t *backend.Task) { t.DataMounts[1].Destination = t.DataMounts[0].Destination }},
		{"cwd-overlap", func(t *backend.Task) { t.Directory = "/mnt/skygo" }},
		{"protected", func(t *backend.Task) { t.DataMounts[0].Destination = "/run/skygo/input" }},
		{"retry", func(t *backend.Task) { t.Retry = &backend.RetryPolicy{MaxAttempts: 2} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := backend.CloneTask(base)
			test.change(&task)
			if checkQualificationTask(r, d, target.Resources, task) == nil {
				t.Fatal("uncovered task accepted")
			}
		})
	}
	if checkQualificationTask(nil, d, target.Resources, base) == nil {
		t.Fatal("nil record accepted")
	}
	copy := cloneQualificationRecord(r)
	copy.Images[0].Profiles = []string{"workdir"}
	if checkQualificationTask(copy, d, target.Resources, base) == nil {
		t.Fatal("uncovered profile accepted")
	}
	copy = cloneQualificationRecord(r)
	copy.Images[0].Mounts = copy.Images[0].Mounts[:1]
	if checkQualificationTask(copy, d, target.Resources, base) == nil {
		t.Fatal("uncovered primitive accepted")
	}
	copy = cloneQualificationRecord(r)
	copy.Images[0].DefaultDirectory = false
	if checkQualificationTask(copy, d, target.Resources, base) == nil {
		t.Fatal("uncovered default cwd accepted")
	}
	task := backend.CloneTask(base)
	task.Directory = "/srv"
	if err := checkQualificationTask(r, d, target.Resources, task); err != nil {
		t.Fatal(err)
	}
}

func TestQualificationTaskDisjointBound(t *testing.T) {
	r, target, transport, actual := qualificationCoverageFixture(t)
	target.Data.References = nil
	r.Images[0].Mounts = nil
	r.Deployment.Storage = nil
	task := qualificationCoverageTask(r)
	task.DataMounts = nil
	for i := 0; i < 16; i++ {
		name, destination := fmt.Sprintf("alias-%02d", i), fmt.Sprintf("/mnt/skygo/path-%02d", i)
		ref := DataReference{Name: name, Destinations: []string{destination}}
		readonly := i%2 == 0
		if readonly {
			ref.Secret = name
			ref.Keys = []string{"key"}
		} else {
			ref.PVC = name
			ref.AllowWrite = true
			r.Deployment.Storage = append(r.Deployment.Storage, qualificationStorage{Reference: name, PVCUID: "uid-" + name, StorageClass: "class", Driver: "csi.example.com", PolicyRevision: "policy-1"})
		}
		target.Data.References = append(target.Data.References, ref)
		r.Images[0].Mounts = append(r.Images[0].Mounts, qualificationMount{Reference: name, Destination: destination, ReadOnly: readonly})
		task.DataMounts = append(task.DataMounts, backend.DataMount{Reference: name, Destination: destination, ReadOnly: readonly})
	}
	refreshQualificationTarget(t, r, target, transport)
	r, err := validateQualificationCoverage(r, target, transport, actual)
	if err != nil {
		t.Fatal(err)
	}
	d, err := newDataProfile(target.Data)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkQualificationTask(r, d, target.Resources, task); err != nil {
		t.Fatal(err)
	}
	// Exactly sixteen independent source-validated primitives, not 65536
	// native subset claims. The seventeenth refuses the portable hard bound.
	task.DataMounts = append(task.DataMounts, backend.DataMount{Reference: "alias-00", Destination: "/mnt/skygo/seventeenth", ReadOnly: true})
	if checkQualificationTask(r, d, target.Resources, task) == nil {
		t.Fatal("seventeenth mount accepted")
	}
}
