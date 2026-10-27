package taskyaml_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/skycompat/taskyaml"
)

func TestCorpus(t *testing.T) {
	data, err := os.ReadFile("../testdata/yaml/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		SchemaVersion int `json:"schema_version"`
		Cases         []struct {
			Name, File, Entrypoint, Expect string
			SHA256                         string  `json:"sha256"`
			ErrorPath                      *string `json:"error_path"`
			ErrorCode                      string  `json:"error_code"`
			Values                         map[string]json.RawMessage
		}
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 1 || len(manifest.Cases) == 0 {
		t.Fatal("missing corpus")
	}
	for _, c := range manifest.Cases {
		t.Run(c.Name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("../testdata/yaml", c.File))
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(data)
			if hex.EncodeToString(hash[:]) != c.SHA256 {
				t.Fatal("fixture hash mismatch")
			}
			parse := taskyaml.Parse
			if c.Entrypoint == "dag" {
				parse = taskyaml.ParseDAG
			}
			d, err := parse(strings.NewReader(string(data)), taskyaml.Options{})
			if c.Expect == "reject" {
				var pe *taskyaml.Error
				if !errors.As(err, &pe) {
					t.Fatalf("want typed refusal, got %v", err)
				}
				if c.ErrorCode != "" && string(pe.Code) != c.ErrorCode {
					t.Errorf("code %s, want %s", pe.Code, c.ErrorCode)
				}
				if c.ErrorPath != nil && pe.Path != *c.ErrorPath {
					t.Errorf("path %q, want %q", pe.Path, *c.ErrorPath)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for key, want := range c.Values {
				var got any
				switch key {
				case "name":
					got = ptr(d.Task.Name)
				case "dag_name":
					got = ptr(d.DAGName)
				case "run":
					got = ptr(d.Task.Run)
				case "setup":
					got = ptr(d.Task.Setup)
				case "workdir":
					got = ptr(d.Task.Workdir)
				case "num_nodes":
					got = d.Task.NumNodes
				case "task_count":
					got = 1
				case "envs":
					got = d.Task.Envs
				case "cpus":
					got = format(d.Task.Resources.CPUs)
				case "memory":
					got = format(d.Task.Resources.Memory)
				case "disk_size":
					got = format(d.Task.Resources.DiskSize)
				case "cloud":
					got = ptr(d.Task.Resources.Cloud)
				case "region":
					got = ptr(d.Task.Resources.Region)
				case "zone":
					got = ptr(d.Task.Resources.Zone)
				case "instance_type":
					got = ptr(d.Task.Resources.InstanceType)
				case "image_id":
					got = ptr(d.Task.Resources.ImageID)
				default:
					t.Fatalf("unhandled corpus assertion %s", key)
				}
				b, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				var gotValue, wantValue any
				if err := json.Unmarshal(b, &gotValue); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(want, &wantValue); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotValue, wantValue) {
					t.Errorf("%s: got %s want %s", key, b, want)
				}
			}
		})
	}
}
func ptr(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}
func format(q *taskyaml.Quantity) string {
	s := q.Value + q.Unit
	if q.AtLeast {
		s += "+"
	}
	return s
}

func TestOverridesAndLocations(t *testing.T) {
	t.Setenv("FROM_HOST", "must not resolve")
	const src = "name: null\nenvs:\n  TOKEN: null\n  NUMBER: 12\nworkdir: $FROM_HOST/project\nrun: |\n  echo '$TOKEN'\nresources:\n  disk_size: 256\n"
	opts := taskyaml.Options{EnvOverrides: map[string]string{"TOKEN": "explicit", "ADDED": "value"}}
	d, err := taskyaml.Parse(strings.NewReader(src), opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := d.Task.Envs; !reflect.DeepEqual(got, map[string]string{"TOKEN": "explicit", "NUMBER": "12", "ADDED": "value"}) {
		t.Fatal(got)
	}
	opts.EnvOverrides["TOKEN"] = "changed"
	if d.Task.Envs["TOKEN"] != "explicit" {
		t.Fatal("document aliases options")
	}
	if *d.Task.Workdir != "$FROM_HOST/project" || *d.Task.Run != "echo '$TOKEN'\n" {
		t.Fatal("parser changed program text")
	}
	if d.Task.Name != nil || d.Locations["name"] != (taskyaml.Position{Line: 1, Column: 7}) {
		t.Fatal("null position lost")
	}
	if d.Locations["resources.disk_size"].Line != 9 || d.Task.Resources.DiskSize.Value != "256" {
		t.Fatal("default disk provenance lost")
	}
}

func TestBoundsAndRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		code        taskyaml.Code
		path        string
	}{
		{"bytes", strings.Repeat(" ", 1<<20+1), taskyaml.Limit, ""},
		{"scalar", "run: " + strings.Repeat("x", 16<<10+1), taskyaml.Limit, "run"},
		{"nodes", "envs: {}\nrun: [" + strings.Repeat("0,", 8200) + "]", taskyaml.Limit, "run"},
		{"nodes overflow", "num_nodes: 2147483648", taskyaml.Invalid, "num_nodes"},
		{"null env", "envs: {PASSWORD: null}", taskyaml.Invalid, "envs"},
		{"unknown secret key", "private-secret-key: secret-value", taskyaml.Unsupported, ""},
		{"custom tag", "run: !secret secret-value", taskyaml.Invalid, "run"},
		{"merge", "run: hi\nenvs: {<<: {A: hi}}", taskyaml.Invalid, "envs"},
		{"map keys", "1: hi", taskyaml.Invalid, ""},
		{"env coercion collision", "envs: {true: x, 'True': y}", taskyaml.Invalid, "envs"},
		{"fractional float env", "envs: {X: 1.5}", taskyaml.Unsupported, "envs"},
		{"resource internal", "resources: {_requires_fuse: false}", taskyaml.Unsupported, "resources._requires_fuse"},
		{"disk minimum", "resources: {disk_size: 512+}", taskyaml.Invalid, "resources.disk_size"},
		{"resource alternatives", "resources: {any_of: [{cpus: 1}]}", taskyaml.Unsupported, "resources.any_of"},
		{"no task doc", "", taskyaml.Invalid, ""},
		{"null task", "null", taskyaml.Invalid, ""},
		{"trailing doc", "run: hi\n---\nrun: next", taskyaml.Unsupported, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := taskyaml.Parse(strings.NewReader(tc.input), taskyaml.Options{})
			var pe *taskyaml.Error
			if !errors.As(err, &pe) {
				t.Fatalf("expected typed error, got %v", err)
			}
			if pe.Code != tc.code || pe.Path != tc.path {
				t.Fatalf("got %+v want %s %q", pe, tc.code, tc.path)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("input leaked")
			}
		})
	}
}

type badReader struct{}

func (badReader) Read([]byte) (int, error) { return 0, fmt.Errorf("secret reader details") }
func TestSafeReaderErrors(t *testing.T) {
	for _, r := range []io.Reader{nil, badReader{}} {
		_, err := taskyaml.Parse(r, taskyaml.Options{})
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal(err)
		}
	}
}

func TestQuantityAndProvenance(t *testing.T) {
	d, err := taskyaml.Parse(strings.NewReader("_user_specified_yaml: 'not interpreted: !anything'\nresources: {cpus: '1.50+', memory: '2mi+', accelerators: {A100: 2}, cloud: imaginary-provider}\n"), taskyaml.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if d.Task.Resources.CPUs.Value != "1.50" || !d.Task.Resources.CPUs.AtLeast || d.Task.Resources.Memory.Unit != "mi" || !d.Task.Resources.Memory.AtLeast {
		t.Fatal("constraints changed")
	}
	if !reflect.DeepEqual(d.Task.Resources.Accelerators, []taskyaml.Accelerator{{Name: "A100", Count: taskyaml.Quantity{Value: "2"}}}) || *d.Task.Resources.Cloud != "imaginary-provider" || *d.SourceYAML != "not interpreted: !anything" {
		t.Fatal("typed references changed")
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{"run: echo hi", "envs: {TOKEN: null}", "run: &x [*x]", "name: null\n---\nrun: hi", "resources: {memory: 1GB+}", "resources: {accelerators: {L4:1, A10G:1}, autostop: {idle_minutes: 5}, job_recovery: {strategy: null}}"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 1<<20 {
			t.Skip()
		}
		for _, parse := range []func(io.Reader, taskyaml.Options) (*taskyaml.Document, error){taskyaml.Parse, taskyaml.ParseDAG} {
			d, err := parse(strings.NewReader(input), taskyaml.Options{})
			if err != nil {
				var pe *taskyaml.Error
				if !errors.As(err, &pe) || d != nil {
					t.Fatalf("invalid error contract: %T %v", err, err)
				}
				continue
			}
			if d == nil || d.Task.NumNodes < 1 {
				t.Fatal("invalid successful document")
			}
		}
	})
}
