package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain lets scripts run this test binary as the skycompat command.
func TestMain(m *testing.M) {
	if os.Getenv("SKYCOMPAT_TEST_HELPER") == "1" {
		os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		stdin      string
		code       int
		stdout     string
		stderr     string
		stderrPart string
	}{
		{name: "no args", code: 2, stderr: usage},
		{name: "help", args: []string{"help"}, stdout: usage},
		{name: "unknown", args: []string{"launch"}, code: 2, stderrPart: `unknown command "launch"`},
		{name: "no file", args: []string{"import", "--shell", "/bin/sh"}, code: 2, stderrPart: usage},
		{name: "two files", args: []string{"import", "a.yaml", "b.yaml"}, code: 2, stderrPart: usage},
		{name: "repeated shell", args: []string{"import", "--shell", "/bin/sh", "--shell", "/bin/bash", "-"}, code: 2, stderrPart: "repeated"},
		{name: "bad timeout", args: []string{"import", "--timeout-seconds", "1h", "-"}, code: 2, stderr: "skycompat: invalid --timeout-seconds\n"},
		{name: "bad env", args: []string{"import", "--env", "NOVALUE", "-"}, code: 2, stderr: "skycompat: --env requires KEY=VALUE\n"},
		{name: "duplicate env", args: []string{"import", "--env", "A=s3cr3t", "--env", "A=s3cr3t", "-"}, code: 2, stderr: "skycompat: duplicate --env key\n"},
		{name: "flag help", args: []string{"import", "-h"}, stderrPart: "-workdir-ref file"},
		{
			name:   "stdin",
			args:   []string{"import", "--shell", "/bin/sh", "--env", "B=2", "-"},
			stdin:  "run: echo $A $B\nenvs: {A: 1}\n",
			stdout: "{\n  \"argv\": [\"/bin/sh\", \"-c\", \"echo $A $B\"],\n  \"env\": {\n    \"A\": \"1\",\n    \"B\": \"2\"\n  }\n}\n",
			stderr: "run\tlowered\targv is shell -c text\nenvs\tlowered\tenvironment\n",
		},
		{name: "refused", args: []string{"import", "--shell", "/bin/sh", "-"}, stdin: "run: x\nnum_nodes: 2\n", code: 1, stderr: "task yaml: unsupported num_nodes at 2:12\n"},
		{name: "parse error", args: []string{"import", "--shell", "/bin/sh", "-"}, stdin: "run: x\nbogus: s3cr3t\n", code: 1, stderr: "task yaml: unsupported at 2:1\n"},
		{name: "option error", args: []string{"import", "--shell", "sh", "-"}, stdin: "run: x\n", code: 1, stderr: "tasklower: shell must be a clean absolute path\n"},
		{name: "missing file", args: []string{"import", "--shell", "/bin/sh", "missing.yaml"}, code: 1, stderrPart: "skycompat: open missing.yaml"},
		{name: "missing ref", args: []string{"import", "--shell", "/bin/sh", "--workdir-ref", "missing.json", "-"}, stdin: "run: x\n", code: 1, stderrPart: "skycompat: open missing.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out, diag bytes.Buffer
			code := run(tt.args, strings.NewReader(tt.stdin), &out, &diag)
			if code != tt.code {
				t.Errorf("exit %d, want %d; stderr:\n%s", code, tt.code, &diag)
			}
			if out.String() != tt.stdout {
				t.Errorf("stdout:\n%s\nwant:\n%s", &out, tt.stdout)
			}
			if tt.stderrPart == "" && diag.String() != tt.stderr || !strings.Contains(diag.String(), tt.stderrPart) {
				t.Errorf("stderr:\n%s\nwant %q", &diag, tt.stderr+tt.stderrPart)
			}
		})
	}
}

func TestReadRef(t *testing.T) {
	const id, sum = "00112233445566778899aabbccddeeff", "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	tests := []struct {
		name, ref string
		ok        bool
	}{
		{"skygo output", `{"id":"` + id + `","sha256":"` + sum + `","size":4096}` + "\n", true},
		{"reordered", `{"size": 1, "sha256": "` + sum + `", "id": "` + id + `"}`, true},
		{"size string", `{"id":"` + id + `","sha256":"` + sum + `","size":"4096"}`, false},
		{"size quoted one", `{"id":"` + id + `","sha256":"` + sum + `","size":"1"}`, false},
		{"size null", `{"id":"` + id + `","sha256":"` + sum + `","size":null}`, false},
		{"size object", `{"id":"` + id + `","sha256":"` + sum + `","size":{}}`, false},
		{"size float", `{"id":"` + id + `","sha256":"` + sum + `","size":4096.0}`, false},
		{"size exponent", `{"id":"` + id + `","sha256":"` + sum + `","size":4e3}`, false},
		{"size leading zero", `{"id":"` + id + `","sha256":"` + sum + `","size":04096}`, false},
		{"duplicate", `{"id":"` + id + `","id":"` + id + `","sha256":"` + sum + `","size":1}`, false},
		{"unknown", `{"id":"` + id + `","sha256":"` + sum + `","size":1,"owner":"x"}`, false},
		{"missing", `{"id":"` + id + `","sha256":"` + sum + `"}`, false},
		{"trailing", `{"id":"` + id + `","sha256":"` + sum + `","size":1}{}`, false},
		{"array", `[]`, false},
		{"too large", `{"id":"` + id + `","sha256":"` + sum + `","size":1}` + strings.Repeat(" ", 4096), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "ref.json")
			if err := os.WriteFile(file, []byte(tt.ref), 0o600); err != nil {
				t.Fatal(err)
			}
			r, err := readRef(file)
			if tt.ok != (err == nil) {
				t.Fatalf("readRef = %v, %v", r, err)
			}
			if err != nil && strings.Contains(err.Error(), id) {
				t.Fatalf("error echoes input: %v", err)
			}
		})
	}
}

// TestImportGate fails if the production packages can reach process
// execution, dynamic loading, gob or pickle decoding, the network, or
// skygo's internal packages.
func TestImportGate(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "./tasklower", "./cmd/skycompat")
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	deps := strings.Fields(string(out))
	if len(deps) < 10 {
		t.Fatalf("go list returned %d packages", len(deps))
	}
	for _, p := range deps {
		switch {
		case p == "os/exec", p == "plugin", p == "encoding/gob", p == "net", strings.HasPrefix(p, "net/"),
			strings.Contains(p, "pickle"), strings.HasPrefix(p, "connectrpc.com/"),
			strings.HasPrefix(p, "github.com/tmc/skygo/internal/"):
			t.Errorf("production dependency %s", p)
		}
	}
	cmd = exec.Command("go", "list", "-f", "{{join .Imports \"\\n\"}}", "./tasklower")
	cmd.Dir = filepath.Join("..", "..")
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, p := range strings.Fields(string(out)) {
		if p == "os" || p == "syscall" || p == "io/fs" || strings.HasPrefix(p, "os/") || p == "path/filepath" {
			t.Errorf("tasklower imports %s", p)
		}
	}
}

// TestNoEnvironment fails if production source reads the process environment.
func TestNoEnvironment(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "tasklower", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, "main.go", "doc.go")
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		ast.Inspect(file, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok {
				if x, ok := s.X.(*ast.Ident); ok && (x.Name == "os" || x.Name == "syscall") {
					switch s.Sel.Name {
					case "Getenv", "LookupEnv", "Environ", "ExpandEnv", "Expand", "Setenv", "Unsetenv", "Clearenv":
						t.Errorf("%s uses %s.%s", f, x.Name, s.Sel.Name)
					}
				}
			}
			return true
		})
	}
	if checked < 4 {
		t.Fatalf("checked %d files", checked)
	}
}
