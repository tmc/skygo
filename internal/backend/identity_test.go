package backend_test

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

func TestTaskBytesGolden(t *testing.T) {
	for _, test := range []struct {
		name         string
		task         backend.Task
		want, digest string
	}{
		{"v1-minimal", backend.Task{Argv: []string{"/bin/echo", "hi"}}, `{"argv":["/bin/echo","hi"]}`, "5540e55137330c64cb3f38acc0975b45aa3a4ddff23dbb3cb800ecb2701b8401"},
		{"v1-all-fields", backend.Task{Argv: []string{"echo"}, Env: map[string]string{"B": "2", "A": "1"}, Directory: "/work", Image: "x", CPUMillis: 100, MemoryBytes: 1024, Accelerator: "nvidia", AcceleratorCount: 1, Target: "cpu-a", TimeoutSeconds: 10}, `{"argv":["echo"],"env":{"A":"1","B":"2"},"directory":"/work","image":"x","cpu_millis":100,"memory_bytes":1024,"accelerator":"nvidia","accelerator_count":1,"target":"cpu-a","timeout_seconds":10}`, "eaffd3fdf54167b5ce804710ff814de32df56ccf1f1a3bce7ae52e2a436449e2"},
		{"limits", backend.Task{Argv: []string{"echo"}, CPUMillis: 100, MemoryBytes: 1024, CPULimitMillis: 200, MemoryLimitBytes: 2048}, `{"argv":["echo"],"cpu_millis":100,"memory_bytes":1024,"cpu_limit_millis":200,"memory_limit_bytes":2048}`, "32d50714cef25faf322ebb9c11ddaa4be8acfd88e7d76c393c5ae19115d6664e"},
		{"placement", backend.Task{Argv: []string{"echo"}, TargetLabels: map[string]string{"location": "lab"}, NodeSelector: map[string]string{"skygo.test/pool": "s04"}}, `{"argv":["echo"],"target_labels":{"location":"lab"},"node_selector":{"skygo.test/pool":"s04"}}`, "b392e01fdf60dfd512a4a7d906ccf355a54aa70d9e59d027011d80c75bb0b743"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := backend.TaskBytes(test.task)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != test.want {
				t.Fatalf("canonical bytes: %s; want %s", got, test.want)
			}
			if digest := fmt.Sprintf("%x", sha256.Sum256(got)); digest != test.digest {
				t.Fatalf("digest %s; want %s", digest, test.digest)
			}
		})
	}
}

func TestTaskBytesLegacyDefaults(t *testing.T) {
	for _, task := range []backend.Task{
		{Argv: []string{"work"}},
		{Argv: []string{"work"}, Env: map[string]string{}, TargetLabels: map[string]string{}, NodeSelector: map[string]string{}},
	} {
		data, err := backend.TaskBytes(task)
		if err != nil || string(data) != `{"argv":["work"]}` {
			t.Fatalf("default encoding changed: %s %v", data, err)
		}
	}
	data, err := backend.TaskBytes(backend.Task{Argv: []string{"é", "<>&"}, Env: map[string]string{"Z": "last", "A": "first"}})
	want := `{"argv":["é","\u003c\u003e\u0026"],"env":{"A":"first","Z":"last"}}`
	if err != nil || string(data) != want {
		t.Fatalf("legacy unicode/escaping changed: %s %v", data, err)
	}
}

func TestLocalBindingGolden(t *testing.T) {
	got, err := backend.LocalBinding("0123456789abcdef0123456789abcdef")
	want := "sha256:144481267d9841772b470124aeae800db7143265da3bb29f98457197c0bce382"
	if err != nil || got != want {
		t.Fatalf("local binding %s %v; want %s", got, err, want)
	}
	other, err := backend.LocalBinding("fedcba9876543210fedcba9876543210")
	if err != nil || other == got {
		t.Fatalf("installation identity not bound: %s %v", other, err)
	}
}

func TestLocalBindingInvalid(t *testing.T) {
	for _, id := range []string{"", "short", strings.Repeat("0", 31), strings.Repeat("0", 33), strings.Repeat("A", 32), strings.Repeat("g", 32)} {
		got, err := backend.LocalBinding(id)
		if err == nil || got != "" || strings.Contains(err.Error(), id) && id != "" {
			t.Fatalf("invalid installation result %q %v", got, err)
		}
	}
}
