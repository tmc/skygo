package kube

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

const testWorkerProfile = `version: 1
target: kube
namespace_uid: ns-uid
service_account_uid: sa-uid
image_pull_secrets: [registry]
helper_image: registry.example.com/skygo/helper@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
helper_sha256: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
budget: {ranks: 16, cpu_millis: 8000, memory_bytes: 17179869184}
`

func TestWorkerConfig(t *testing.T) {
	cfg, err := readWorkerConfig([]byte(testWorkerProfile))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Target() != "kube" || cfg.namespaceUID != "ns-uid" || cfg.registry.ServiceAccountUID != "sa-uid" ||
		!reflect.DeepEqual(cfg.registry.PullSecrets, []string{"registry"}) || cfg.budget.Ranks != 16 ||
		cfg.budget.CPUMillis != 8000 || cfg.budget.MemoryBytes != 17179869184 || cfg.budget.Accelerators != 0 ||
		cfg.helperImage != "registry.example.com/skygo/helper@sha256:"+strings.Repeat("a", 64) || cfg.helperSHA256 != strings.Repeat("b", 64) {
		t.Fatalf("profile differs: %+v", cfg)
	}
	for _, tt := range []struct{ name, old, replacement string }{
		{"minimum ranks", "ranks: 16", "ranks: 2"},
		{"maximum ranks", "ranks: 16", "ranks: 2048"},
		{"minimum resources", "cpu_millis: 8000, memory_bytes: 17179869184", "cpu_millis: 1, memory_bytes: 1"},
		{"no pull secrets", "image_pull_secrets: [registry]\n", ""},
		{"empty pull secrets", "image_pull_secrets: [registry]", "image_pull_secrets: []"},
		{"quoted names", "target: kube", "target: 'kube'"},
		{"ordered pull secrets", "image_pull_secrets: [registry]", "image_pull_secrets: [z, a]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readWorkerConfig([]byte(strings.Replace(testWorkerProfile, tt.old, tt.replacement, 1)))
			if err != nil || got.check() != nil {
				t.Fatalf("valid profile refused: %v", err)
			}
			if tt.name == "ordered pull secrets" && !reflect.DeepEqual(got.registry.PullSecrets, []string{"z", "a"}) {
				t.Fatal("pull secret order changed")
			}
		})
	}
}

func TestWorkerConfigRefused(t *testing.T) {
	for _, tt := range []struct{ name, old, replacement string }{
		{"future version", "version: 1", "version: 2"},
		{"missing version", "version: 1\n", ""},
		{"quoted version", "version: 1", `version: "1"`},
		{"boolean version", "version: 1", "version: true"},
		{"explicit integer tag", "version: 1", "version: !!int 1"},
		{"zero version", "version: 1", "version: 0"},
		{"octal version", "version: 1", "version: 01"},
		{"hex version", "version: 1", "version: 0x1"},
		{"signed version", "version: 1", "version: +1"},
		{"float version", "version: 1", "version: 1.0"},
		{"duplicate version", "version: 1", "version: 1\nversion: 1"},
		{"unknown root", "target: kube", "target: kube\nunknown: x"},
		{"missing target", "target: kube\n", ""},
		{"local target", "target: kube", "target: local"},
		{"uppercase target", "target: kube", "target: Kube"},
		{"target coercion", "target: kube", "target: 123"},
		{"target null", "target: kube", "target: null"},
		{"target block scalar", "target: kube", "target: |-\n  kube"},
		{"target explicit string tag", "target: kube", "target: !!str kube"},
		{"map target", "target: kube", "target: {}"},
		{"namespace missing", "namespace_uid: ns-uid\n", ""},
		{"namespace empty", "namespace_uid: ns-uid", "namespace_uid: ''"},
		{"namespace spaces", "namespace_uid: ns-uid", "namespace_uid: 'ns uid'"},
		{"namespace control", "namespace_uid: ns-uid", `namespace_uid: "ns\nuid"`},
		{"service account missing", "service_account_uid: sa-uid\n", ""},
		{"service account empty", "service_account_uid: sa-uid", "service_account_uid: ''"},
		{"pull secret null", "image_pull_secrets: [registry]", "image_pull_secrets: null"},
		{"pull secret scalar", "image_pull_secrets: [registry]", "image_pull_secrets: registry"},
		{"pull secret map", "image_pull_secrets: [registry]", "image_pull_secrets: {}"},
		{"pull secret duplicate", "image_pull_secrets: [registry]", "image_pull_secrets: [registry, registry]"},
		{"pull secret empty", "image_pull_secrets: [registry]", "image_pull_secrets: ['']"},
		{"pull secret boolean", "image_pull_secrets: [registry]", "image_pull_secrets: [true]"},
		{"pull secret nested", "image_pull_secrets: [registry]", "image_pull_secrets: [[registry]]"},
		{"pull secret foreign name", "image_pull_secrets: [registry]", "image_pull_secrets: [Registry]"},
		{"pull secret anchor", "image_pull_secrets: [registry]", "image_pull_secrets: [&secret registry]"},
		{"helper missing", "helper_image: registry.example.com/skygo/helper@sha256:" + strings.Repeat("a", 64) + "\n", ""},
		{"helper tag", "helper@sha256:" + strings.Repeat("a", 64), "helper:latest"},
		{"helper no registry", "registry.example.com/skygo/helper", "helper"},
		{"helper uppercase digest", strings.Repeat("a", 64), strings.Repeat("A", 64)},
		{"helper digest missing", "helper_sha256: " + strings.Repeat("b", 64) + "\n", ""},
		{"helper digest short", strings.Repeat("b", 64), strings.Repeat("b", 63)},
		{"helper digest uppercase", strings.Repeat("b", 64), strings.Repeat("B", 64)},
		{"budget missing", "budget: {ranks: 16, cpu_millis: 8000, memory_bytes: 17179869184}\n", ""},
		{"budget null", "budget: {ranks: 16, cpu_millis: 8000, memory_bytes: 17179869184}", "budget: null"},
		{"budget empty", "budget: {ranks: 16, cpu_millis: 8000, memory_bytes: 17179869184}", "budget: {}"},
		{"budget sequence", "budget: {ranks: 16, cpu_millis: 8000, memory_bytes: 17179869184}", "budget: []"},
		{"rank absent", "ranks: 16, ", ""},
		{"rank zero", "ranks: 16", "ranks: 0"},
		{"rank one", "ranks: 16", "ranks: 1"},
		{"rank too many", "ranks: 16", "ranks: 2049"},
		{"rank negative", "ranks: 16", "ranks: -1"},
		{"rank underscore", "ranks: 16", "ranks: 1_6"},
		{"rank overflow", "ranks: 16", "ranks: 18446744073709551616"},
		{"cpu absent", "cpu_millis: 8000, ", ""},
		{"cpu zero", "cpu_millis: 8000", "cpu_millis: 0"},
		{"cpu quoted", "cpu_millis: 8000", `cpu_millis: "8000"`},
		{"cpu floating", "cpu_millis: 8000", "cpu_millis: 8e3"},
		{"memory absent", ", memory_bytes: 17179869184", ""},
		{"memory zero", "memory_bytes: 17179869184", "memory_bytes: 0"},
		{"accelerator budget", "ranks: 16", "accelerators: 0, ranks: 16"},
		{"duplicate budget quantity", "ranks: 16", "ranks: 16, ranks: 16"},
		{"data block", "target: kube", "target: kube\ndata: {}"},
		{"environment block", "target: kube", "target: kube\nenvironments: {}"},
		{"alias", "target: kube", "target: &target kube\nother: *target"},
		{"merge", "target: kube", "target: kube\n<<: {target: kube}"},
		{"key anchor", "target: kube", "&key target: kube"},
		{"complex key", "target: kube", "? [target]\n: kube"},
		{"root anchor", "version: 1", "&profile\nversion: 1"},
		{"second document", "version: 1", "version: 1\n---\nversion: 1"},
		{"nul", "target: kube", "target: ku\x00be"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			doc := strings.Replace(testWorkerProfile, tt.old, tt.replacement, 1)
			if doc == testWorkerProfile {
				t.Fatal("case did not change document")
			}
			cfg, err := readWorkerConfig([]byte(doc))
			if err == nil || !reflect.DeepEqual(cfg, WorkerConfig{}) {
				t.Fatalf("refused profile = %+v, %v", cfg, err)
			}
		})
	}
	for _, data := range [][]byte{nil, []byte("[]"), []byte("null"), []byte("---"), []byte(testWorkerProfile + "---\n"), []byte(testWorkerProfile + "---\n" + testWorkerProfile), []byte(strings.Repeat(" ", maxWorkerConfigBytes+1))} {
		if _, err := readWorkerConfig(data); err == nil {
			t.Fatal("invalid root or size accepted")
		}
	}
}

func TestWorkerConfigChecksAndSnapshots(t *testing.T) {
	cfg, err := readWorkerConfig([]byte(testWorkerProfile))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		edit func(*WorkerConfig)
	}{
		{"zero", func(c *WorkerConfig) { *c = WorkerConfig{} }},
		{"namespace", func(c *WorkerConfig) { c.namespaceUID = "" }},
		{"account", func(c *WorkerConfig) { c.registry.ServiceAccountUID = "" }},
		{"duplicate secrets", func(c *WorkerConfig) { c.registry.PullSecrets = []string{"x", "x"} }},
		{"too many secrets", func(c *WorkerConfig) {
			c.registry.PullSecrets = nil
			for i := 0; i < 33; i++ {
				c.registry.PullSecrets = append(c.registry.PullSecrets, fmt.Sprintf("s%d", i))
			}
		}},
		{"helper", func(c *WorkerConfig) { c.helperImage = "helper:latest" }},
		{"digest", func(c *WorkerConfig) { c.helperSHA256 = "bad" }},
		{"ranks", func(c *WorkerConfig) { c.budget.Ranks = 1 }},
		{"cpu", func(c *WorkerConfig) { c.budget.CPUMillis = 0 }},
		{"memory", func(c *WorkerConfig) { c.budget.MemoryBytes = 0 }},
		{"accelerator", func(c *WorkerConfig) { c.budget.Accelerators = 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			copy := cfg.clone()
			tt.edit(&copy)
			if err := copy.check(); err == nil {
				t.Fatal("invalid internal profile accepted")
			}
		})
	}
	before := cfg.clone()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copy := cfg.clone()
			copy.registry.PullSecrets[0] = "changed"
			copy.target = "changed"
			copy.budget.Ranks = 2
		}()
	}
	wg.Wait()
	if !reflect.DeepEqual(cfg, before) {
		t.Fatal("clones changed original snapshot")
	}
}

func TestWorkerConfigFile(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("private profile files require Linux or Darwin")
	}
	dir := t.TempDir()
	for _, mode := range []os.FileMode{0o400, 0o600} {
		name := filepath.Join(dir, fmt.Sprintf("valid-%o", mode))
		if err := os.WriteFile(name, []byte(testWorkerProfile), mode); err != nil {
			t.Fatal(err)
		}
		cfg, err := ReadWorkerConfig(name)
		if err != nil || cfg.Target() != "kube" {
			t.Fatalf("private profile: %+v, %v", cfg, err)
		}
	}
	valid := filepath.Join(dir, "valid-600")
	link := filepath.Join(dir, "symlink")
	if err := os.Symlink(valid, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadWorkerConfig(link); err == nil {
		t.Fatal("symlink accepted")
	}
	hard := filepath.Join(dir, "hardlink")
	if err := os.Link(valid, hard); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadWorkerConfig(hard); err == nil {
		t.Fatal("hardlink accepted")
	}
	for _, mode := range []os.FileMode{0o644, 0o640, 0o660, 0o700} {
		name := filepath.Join(dir, fmt.Sprintf("unsafe-%o", mode))
		if err := os.WriteFile(name, []byte(testWorkerProfile), mode); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadWorkerConfig(name); err == nil {
			t.Fatalf("mode %o accepted", mode)
		}
	}
	for _, tt := range []struct{ name, data string }{
		{"empty", ""}, {"oversize", strings.Repeat("x", maxWorkerConfigBytes+1)}, {"malformed", "version: ["},
	} {
		name := filepath.Join(dir, tt.name)
		if err := os.WriteFile(name, []byte(tt.data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadWorkerConfig(name); err == nil {
			t.Fatalf("%s accepted", tt.name)
		}
	}
	for _, name := range []string{dir, filepath.Join(dir, "missing")} {
		if _, err := ReadWorkerConfig(name); err == nil {
			t.Fatal("non-file accepted")
		}
	}
}

// This is a constructed error-chain test, not an injected OS read or Close
// failure. The real ordinary file is closed once; neither synthetic cause is
// evidence that its read or Close failed.
func TestWorkerConfigModeledReadCloseErrorChain(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "profile-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	readCause := errors.New("private original read cause")
	closeCause := errors.New("private original close cause")
	read := &workerConfigReadError{cause: readCause}
	if read.Error() != "read worker profile" || !errors.Is(read, readCause) {
		t.Fatal("read error lost its original cause or safe text")
	}
	joined := &workerConfigCloseError{file: f, cause: errors.Join(read, closeCause)}
	if joined.Error() != "close worker profile" || joined.file != f ||
		!errors.Is(joined, readCause) || !errors.Is(joined, closeCause) {
		t.Fatal("joined error lost its original file, causes or safe text")
	}
}

func ExampleWorkerConfig() {
	var profile WorkerConfig
	fmt.Printf("target: %q\n", profile.Target())
	// Output: target: ""
}

func ExampleWorkerConfig_Target() {
	profile, err := readWorkerConfig([]byte(testWorkerProfile))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(profile.Target())
	// Output: kube
}

func ExampleReadWorkerConfig() {
	dir, err := os.MkdirTemp("", "worker-profile-")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	name := filepath.Join(dir, "workers.yaml")
	if err := os.WriteFile(name, []byte(testWorkerProfile), 0o600); err != nil {
		fmt.Println(err)
		return
	}
	profile, err := ReadWorkerConfig(name)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(profile.Target())
	// Output: kube
}
