package kube

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// These records are codec fixtures, not results of a native qualification run.
func qualificationFixture() *qualificationRecord {
	h := strings.Repeat("a", 64)
	return &qualificationRecord{
		Version: qualificationRecordVersion, Composition: qualificationComposition, TargetSHA256: h,
		Implementation: qualificationImplementation{ExecutableSHA256: h, BuildReceiptSHA256: h, HelperImage: "registry.example.com/helper@sha256:" + h, HelperSHA256: h, HelperPlatformDigest: h, HelperOS: "linux", HelperArchitecture: "arm64", OS: "darwin", Architecture: "arm64"},
		Deployment:     qualificationDeployment{APIServer: "https://kube.example.com", CASHA256: h, Namespace: "qualification", NamespaceUID: "namespace-uid", KubernetesVersion: "v1.32.3", Pool: qualificationPool{Key: "skygo.example.com/pool", Value: "qualified"}, PolicyRevision: "operator-policy-1", Nodes: []qualificationNode{{Name: "node-1", UID: "node-uid", Kubelet: "v1.32.3", Runtime: "containerd://2.0", Kernel: "6.8.0", OS: "linux", Architecture: "arm64"}}, Storage: []qualificationStorage{{Reference: "data", PVCUID: "pvc-uid", StorageClass: "local", Driver: "example.com/csi", PolicyRevision: "storage-1"}}},
		Images:         []qualificationImage{{Image: "registry.example.com/task@sha256:" + h, PlatformDigest: h, OS: "linux", Architecture: "arm64", Profiles: []string{"mount-only", "workdir", "workdir-output", "workdir-setup", "workdir-setup-output"}, FixedPathsSHA256: h, DefaultDirectory: true, Directories: []string{"/srv"}, Mounts: []qualificationMount{{Reference: "data", Destination: "/mnt/skygo/data", ReadOnly: false}, {Reference: "data", Destination: "/mnt/skygo/data", ReadOnly: true}}}},
		Evidence:       qualificationEvidence{RunID: strings.Repeat("b", 32), CompletedAt: time.Date(2026, 10, 20, 1, 2, 3, 4, time.UTC), ReceiptSHA256: h, Cases: []qualificationCase{{Name: "fixture-only-not-native", ReceiptSHA256: h}}},
	}
}

func TestQualificationRecordCanonical(t *testing.T) {
	r := qualificationFixture()
	b, err := encodeQualificationRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeQualificationRecord(b)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("round trip: %v", err)
	}
	again, err := encodeQualificationRecord(got)
	if err != nil || !bytes.Equal(b, again) {
		t.Fatalf("reencode: %v", err)
	}
	id, err := qualificationRecordID(r)
	want := sha256.Sum256(b)
	if err != nil || id != hex.EncodeToString(want[:]) {
		t.Fatalf("identity=%s err=%v", id, err)
	}
	got.Deployment.PolicyRevision = "changed"
	other, err := qualificationRecordID(got)
	if err != nil || other == id {
		t.Fatalf("policy drift did not change identity: %v", err)
	}
	for name, raw := range map[string][]byte{
		"empty": nil, "null": []byte("null"), "space": append([]byte(" "), b...),
		"newline": append(bytes.Clone(b), '\n'), "trailing": append(bytes.Clone(b), []byte("{}")...),
		"unknown":   append([]byte(`{"extra":0,`), b[1:]...),
		"duplicate": append([]byte(`{"version":"ignored",`), b[1:]...),
		"oversize":  bytes.Repeat([]byte(" "), maxQualificationRecord+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeQualificationRecord(raw); !errors.Is(err, errQualificationRecord) {
				t.Fatalf("decode error=%v", err)
			}
		})
	}
}

func TestQualificationRecordValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*qualificationRecord)
	}{
		{"version", func(r *qualificationRecord) { r.Version = "future" }},
		{"composition", func(r *qualificationRecord) { r.Composition = "unreviewed" }},
		{"target", func(r *qualificationRecord) { r.TargetSHA256 = strings.Repeat("A", 64) }},
		{"helper", func(r *qualificationRecord) { r.Implementation.HelperImage = "task:latest" }},
		{"platform", func(r *qualificationRecord) { r.Implementation.HelperPlatformDigest = "" }},
		{"api", func(r *qualificationRecord) { r.Deployment.APIServer = "http://kube.example.com" }},
		{"namespace", func(r *qualificationRecord) { r.Deployment.NamespaceUID = "" }},
		{"pool", func(r *qualificationRecord) { r.Deployment.Pool.Value = "" }},
		{"pool-key", func(r *qualificationRecord) { r.Deployment.Pool.Key = "bad key" }},
		{"nodes", func(r *qualificationRecord) { r.Deployment.Nodes = nil }},
		{"duplicate-node", func(r *qualificationRecord) { r.Deployment.Nodes = append(r.Deployment.Nodes, r.Deployment.Nodes[0]) }},
		{"duplicate-uid", func(r *qualificationRecord) {
			n := r.Deployment.Nodes[0]
			n.Name = "node-2"
			r.Deployment.Nodes = append(r.Deployment.Nodes, n)
		}},
		{"runtime", func(r *qualificationRecord) { r.Deployment.Nodes[0].Runtime = "" }},
		{"storage", func(r *qualificationRecord) { r.Deployment.Storage[0].PolicyRevision = "" }},
		{"duplicate-storage", func(r *qualificationRecord) {
			r.Deployment.Storage = append(r.Deployment.Storage, r.Deployment.Storage[0])
		}},
		{"images", func(r *qualificationRecord) { r.Images = nil }},
		{"duplicate-image", func(r *qualificationRecord) { r.Images = append(r.Images, r.Images[0]) }},
		{"profile", func(r *qualificationRecord) { r.Images[0].Profiles = []string{"retry"} }},
		{"duplicate-profile", func(r *qualificationRecord) { r.Images[0].Profiles = []string{"workdir", "workdir"} }},
		{"relative", func(r *qualificationRecord) { r.Images[0].Directories = []string{"srv"} }},
		{"unclean", func(r *qualificationRecord) { r.Images[0].Directories = []string{"/srv/../srv"} }},
		{"duplicate-directory", func(r *qualificationRecord) { r.Images[0].Directories = []string{"/srv", "/srv"} }},
		{"alias", func(r *qualificationRecord) { r.Images[0].Mounts[0].Reference = "bad/name" }},
		{"destination", func(r *qualificationRecord) { r.Images[0].Mounts[0].Destination = "/run/skygo/input" }},
		{"duplicate-primitive", func(r *qualificationRecord) { r.Images[0].Mounts[1] = r.Images[0].Mounts[0] }},
		{"primitive-order", func(r *qualificationRecord) {
			r.Images[0].Mounts[0], r.Images[0].Mounts[1] = r.Images[0].Mounts[1], r.Images[0].Mounts[0]
		}},
		{"run", func(r *qualificationRecord) { r.Evidence.RunID = "" }},
		{"time", func(r *qualificationRecord) { r.Evidence.CompletedAt = time.Time{} }},
		{"zone", func(r *qualificationRecord) {
			r.Evidence.CompletedAt = r.Evidence.CompletedAt.In(time.FixedZone("offset", 3600))
		}},
		{"cases", func(r *qualificationRecord) { r.Evidence.Cases = nil }},
		{"duplicate-case", func(r *qualificationRecord) { r.Evidence.Cases = append(r.Evidence.Cases, r.Evidence.Cases[0]) }},
		{"text", func(r *qualificationRecord) { r.Evidence.Cases[0].Name = "bad\ntext" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := qualificationFixture()
			test.change(r)
			if _, err := encodeQualificationRecord(r); !errors.Is(err, errQualificationRecord) {
				t.Fatalf("encode error=%v", err)
			}
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeQualificationRecord(b); !errors.Is(err, errQualificationRecord) {
				t.Fatalf("decode error=%v", err)
			}
		})
	}
}

func TestQualificationRecordCopies(t *testing.T) {
	r := qualificationFixture()
	before, err := encodeQualificationRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	c := cloneQualificationRecord(r)
	c.Deployment.Nodes[0].UID = "changed"
	c.Deployment.Storage[0].PVCUID = "changed"
	c.Images[0].Profiles[0] = "changed"
	c.Images[0].Directories[0] = "/changed"
	c.Images[0].Mounts[0].Reference = "changed"
	c.Evidence.Cases[0].Name = "changed"
	after, err := encodeQualificationRecord(r)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("source mutated: %v", err)
	}
	if cloneQualificationRecord(nil) != nil {
		t.Fatal("nil clone")
	}
}

func TestQualificationRecordCaps(t *testing.T) {
	for _, count := range []int{32, 33} {
		r := qualificationFixture()
		r.Deployment.Nodes = nil
		for i := 0; i < count; i++ {
			n := qualificationFixture().Deployment.Nodes[0]
			n.Name = fmt.Sprintf("node-%02d", i)
			n.UID = n.Name
			r.Deployment.Nodes = append(r.Deployment.Nodes, n)
		}
		_, err := encodeQualificationRecord(r)
		if (err == nil) != (count == 32) {
			t.Fatalf("nodes=%d err=%v", count, err)
		}
	}
	// All individual tables are legal, but they do not promise their Cartesian
	// maximum fits the fixed whole-record byte cap.
	r := qualificationFixture()
	r.Images = nil
	for i := 0; i < 32; i++ {
		image := qualificationFixture().Images[0]
		image.Image = fmt.Sprintf("registry.example.com/task%02d@sha256:%064x", i, i+1)
		image.Mounts = nil
		for ref := 0; ref < 16; ref++ {
			for dst := 0; dst < 16; dst++ {
				for _, ro := range []bool{false, true} {
					image.Mounts = append(image.Mounts, qualificationMount{Reference: fmt.Sprintf("ref-%02d", ref), Destination: fmt.Sprintf("/mnt/skygo/path-%02d", dst), ReadOnly: ro})
				}
			}
		}
		r.Images = append(r.Images, image)
	}
	if !validQualificationRecord(r) {
		t.Fatal("boundary profile invalid before byte cap")
	}
	b, err := json.Marshal(r)
	if err != nil || len(b) <= maxQualificationRecord {
		t.Fatalf("bytes=%d err=%v", len(b), err)
	}
	if _, err := encodeQualificationRecord(r); !errors.Is(err, errQualificationRecord) {
		t.Fatalf("oversized encode=%v", err)
	}
}

func TestQualificationRecordFile(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("supported loader platform required")
	}
	fixture := newQualificationRecordFixture(t, 0)
	dir := fixture.directory
	name := filepath.Join(dir, "record.json")
	b, err := encodeQualificationRecord(qualificationFixture())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, b, 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := fixture.load(t, name)
	if err != nil || !reflect.DeepEqual(r, qualificationFixture()) {
		t.Fatalf("load: %v", err)
	}
	if err := os.Chmod(name, 0o400); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.load(t, name); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(name, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.load(t, name); err == nil {
		t.Fatal("group-readable file accepted")
	}
	if err := os.Chmod(name, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(name, link); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.load(t, link); err == nil {
		t.Fatal("symlink accepted")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(name, link); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.load(t, name); err == nil {
		t.Fatal("hard-linked file accepted")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.load(t, dir); err == nil {
		t.Fatal("directory accepted")
	}
	if err := os.WriteFile(name, bytes.Repeat([]byte("x"), maxQualificationRecord+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.load(t, name); err == nil {
		t.Fatal("oversized file accepted")
	}
	if err := os.WriteFile(name, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.load(t, name); err == nil {
		t.Fatal("malformed file accepted")
	}
}

// A persistent fixture retains concrete original custody on Close uncertainty.
type qualificationRecordFixture struct {
	directory string
	file      *os.File
	cause     error
	slot      int
	previous  *qualificationRecordFixture
}

// Two concrete test roots retain their original frames, including repeated
// invocations with -count. Tests are not parallel. Do not retry Close or let
// testing remove a namespace while its original file closure is uncertain.
var qualificationRecordFixtures [2]*qualificationRecordFixture

func newQualificationRecordFixture(t *testing.T, slot int) *qualificationRecordFixture {
	t.Helper()
	base := os.Getenv("SKYGO_QUALIFICATION_TEST_DIR")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		base = filepath.Join(home, "tmp", "skygo", time.Now().UTC().Format("20060102")+"-qualification-record-tests")
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(base, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &qualificationRecordFixture{directory: directory, slot: slot}
	t.Cleanup(func() {
		if fixture.cause != nil {
			return
		}
		if err := os.RemoveAll(fixture.directory); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

func (f *qualificationRecordFixture) retain(file *os.File, cause error) {
	f.file, f.cause = file, cause
	if qualificationRecordFixtures[f.slot] != f {
		f.previous = qualificationRecordFixtures[f.slot]
		qualificationRecordFixtures[f.slot] = f
	}
}

func (f *qualificationRecordFixture) load(t *testing.T, name string) (*qualificationRecord, error) {
	t.Helper()
	r, err := loadQualificationRecord(name)
	var held *qualificationFileError
	if errors.As(err, &held) {
		f.retain(held.file, err)
		t.Fatal("original qualification file close remains uncertain; fixture retained")
	}
	return r, err
}

func TestQualificationRecordOriginalCloseError(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("supported loader platform required")
	}
	fixture := newQualificationRecordFixture(t, 1)
	previous := qualificationRecordFixtures[1]
	f, err := os.CreateTemp(fixture.directory, "original")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		fixture.retain(f, err)
		t.Fatal("first original close failed; fixture retained")
	}
	r, err := readQualificationRecord(f)
	var held *qualificationFileError
	if err != nil {
		fixture.retain(f, err)
	}
	if fixture.previous != previous {
		t.Fatal("previous original fixture custody lost")
	}
	if r != nil || !errors.As(err, &held) || held.file != f || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("original file/error lost: record=%v err=%v", r, err)
	}
	if strings.Contains(err.Error(), f.Name()) {
		t.Fatal("close diagnostic exposes path")
	}
}

func TestQualificationRecordTableBoundaries(t *testing.T) {
	for _, test := range []struct {
		name     string
		limit    int
		populate func(*qualificationRecord, int)
	}{
		{"images", 32, func(r *qualificationRecord, count int) {
			base := r.Images[0]
			r.Images = nil
			for i := 0; i < count; i++ {
				image := base
				image.Image = fmt.Sprintf("registry.example.com/task%02d@sha256:%064x", i, i+1)
				r.Images = append(r.Images, image)
			}
		}},
		{"storage", 16, func(r *qualificationRecord, count int) {
			base := r.Deployment.Storage[0]
			r.Deployment.Storage = nil
			for i := 0; i < count; i++ {
				storage := base
				storage.Reference = fmt.Sprintf("ref-%02d", i)
				r.Deployment.Storage = append(r.Deployment.Storage, storage)
			}
		}},
		{"directories", 256, func(r *qualificationRecord, count int) {
			r.Images[0].Directories = nil
			for i := 0; i < count; i++ {
				r.Images[0].Directories = append(r.Images[0].Directories, fmt.Sprintf("/srv/path-%03d", i))
			}
		}},
		{"mounts", 512, func(r *qualificationRecord, count int) {
			r.Images[0].Mounts = nil
			for i := 0; i < count; i++ {
				r.Images[0].Mounts = append(r.Images[0].Mounts, qualificationMount{Reference: fmt.Sprintf("ref-%02d", i/32), Destination: fmt.Sprintf("/mnt/skygo/path-%02d", i%32/2), ReadOnly: i%2 != 0})
			}
		}},
		{"cases", 256, func(r *qualificationRecord, count int) {
			base := r.Evidence.Cases[0]
			r.Evidence.Cases = nil
			for i := 0; i < count; i++ {
				c := base
				c.Name = fmt.Sprintf("case-%03d", i)
				r.Evidence.Cases = append(r.Evidence.Cases, c)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, count := range []int{test.limit, test.limit + 1} {
				r := qualificationFixture()
				test.populate(r, count)
				b, err := encodeQualificationRecord(r)
				if (err == nil) != (count == test.limit) {
					t.Fatalf("count=%d err=%v", count, err)
				}
				if err == nil {
					if _, err := decodeQualificationRecord(b); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
