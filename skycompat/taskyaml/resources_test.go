package taskyaml_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/skycompat/taskyaml"
)

func str(s string) *string { return &s }
func i64(x int64) *int64   { return &x }
func boolp(b bool) *bool   { return &b }

func one(name string) taskyaml.Accelerator {
	return taskyaml.Accelerator{Name: name, Count: taskyaml.Quantity{Value: "1"}}
}

// TestPrimer parses unmodified primer examples (see testdata/primer/README)
// and requires every field to survive in the typed document.
func TestPrimer(t *testing.T) {
	tests := []struct {
		file, sha256 string
		want         taskyaml.Task
		locations    map[string]taskyaml.Position
	}{
		{
			file:   "01-hello.yaml",
			sha256: "11cec7478465ae9f2b23a77477ebccac537f33049a7b2f229e1bfbf174a515dc",
			want: taskyaml.Task{
				Name: str("hello"),
				Run:  str("echo \"node $SKYPILOT_NODE_RANK of $SKYPILOT_NUM_NODES\"\necho \"$SKYPILOT_CLUSTER_INFO\"\nnproc; free -g\n"),
				Resources: &taskyaml.Resources{
					CPUs:     &taskyaml.Quantity{Value: "2", AtLeast: true},
					Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(10)},
				},
				NumNodes: 1,
			},
			locations: map[string]taskyaml.Position{
				"name":               {Line: 7, Column: 7},
				"resources.cpus":     {Line: 10, Column: 9},
				"resources.autostop": {Line: 11, Column: 13},
			},
		},
		{
			file:   "02-go-cuda-dev.yaml",
			sha256: "2f60b6192ae0b11681d8a82ec0bc4725b9ba2a7ae9f0def5d6f0f83edb99b764",
			want: taskyaml.Task{
				Name:    str("gocuda"),
				Workdir: str("."),
				Envs:    map[string]string{"GO_VERSION": "1.27.2"},
				Setup: str(`set -e
if ! command -v go >/dev/null || ! go version | grep -q "go$GO_VERSION"; then
  curl -fsSL "https://go.dev/dl/go$GO_VERSION.linux-amd64.tar.gz" | sudo tar -C /usr/local -xz
fi
echo 'export PATH=/usr/local/go/bin:$HOME/go/bin:$PATH' >> ~/.bashrc
sudo apt-get update -qq && sudo apt-get install -y -qq git build-essential >/dev/null
`),
				Run: str(`export PATH=/usr/local/go/bin:$PATH
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv
nvcc --version | tail -1
go version
go env GOARCH GOOS
`),
				Resources: &taskyaml.Resources{
					Accelerators: []taskyaml.Accelerator{one("L4"), one("A10G"), one("L40S")},
					ImageID:      str("docker:nvidia/cuda:13.4.3-devel-ubuntu24.04"),
					DiskSize:     &taskyaml.Quantity{Value: "128"},
					Autostop:     &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(30), WaitFor: str("jobs_and_ssh")},
				},
				NumNodes: 1,
			},
			locations: map[string]taskyaml.Position{
				"resources.accelerators": {Line: 15, Column: 17},
				"resources.image_id":     {Line: 16, Column: 13},
				"resources.disk_size":    {Line: 17, Column: 14},
				"resources.autostop":     {Line: 19, Column: 5},
			},
		},
		{
			file:   "09-dgx-gpu.yaml",
			sha256: "b5848e3fcccad77ef11a1995851ebf24bbb66a72508abec2394e4b0808759094",
			want: taskyaml.Task{
				Name: str("spark"),
				Run:  str("uname -m\nnvidia-smi --query-gpu=name,driver_version --format=csv\necho \"rank $SKYPILOT_NODE_RANK of $SKYPILOT_NUM_NODES, $SKYPILOT_NUM_GPUS_PER_NODE GPU\"\n"),
				Resources: &taskyaml.Resources{
					Infra:        str("ssh/dgx"),
					Accelerators: []taskyaml.Accelerator{{Name: "GB10", Count: taskyaml.Quantity{Value: "1"}}},
				},
				NumNodes: 1,
			},
			locations: map[string]taskyaml.Position{
				"resources.infra":        {Line: 20, Column: 10},
				"resources.accelerators": {Line: 21, Column: 17},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			b, err := os.ReadFile(filepath.Join("testdata", "primer", tt.file))
			if err != nil {
				t.Fatal(err)
			}
			if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != tt.sha256 {
				t.Fatal("primer copy differs from its pinned original")
			}
			d, err := taskyaml.Parse(bytes.NewReader(b), taskyaml.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if d.DAGName != nil || d.SourceYAML != nil {
				t.Error("unexpected DAG name or provenance")
			}
			if !reflect.DeepEqual(d.Task, tt.want) {
				t.Errorf("task =\n%#v\nwant\n%#v", d.Task, tt.want)
				if g, w := d.Task.Resources, tt.want.Resources; g != nil && w != nil && !reflect.DeepEqual(*g, *w) {
					t.Errorf("resources =\n%#v\nwant\n%#v", *g, *w)
				}
			}
			for p, want := range tt.locations {
				if got := d.Locations[p]; got != want {
					t.Errorf("location %s = %v, want %v", p, got, want)
				}
			}
		})
	}
}

// TestAccelerators covers the upstream accelerators forms that
// Resources.from_yaml_config reads (sky/resources.py).
func TestAccelerators(t *testing.T) {
	tests := []struct {
		name, yaml string
		want       []taskyaml.Accelerator
		ordered    bool
	}{
		{"name", "T4", []taskyaml.Accelerator{one("T4")}, false},
		{"name count", "'A100:8'", []taskyaml.Accelerator{{Name: "A100", Count: taskyaml.Quantity{Value: "8"}}}, false},
		{"fractional count", "'T4:0.5'", []taskyaml.Accelerator{{Name: "T4", Count: taskyaml.Quantity{Value: "0.5"}}}, false},
		{"standard map", "{H100: 8}", []taskyaml.Accelerator{{Name: "H100", Count: taskyaml.Quantity{Value: "8"}}}, false},
		{"standard map choices", "{L4: 1, A10G: 2.5}", []taskyaml.Accelerator{one("L4"), {Name: "A10G", Count: taskyaml.Quantity{Value: "2.5"}}}, false},
		{"no-space set", "{L4:1, A10G:2}", []taskyaml.Accelerator{one("L4"), {Name: "A10G", Count: taskyaml.Quantity{Value: "2"}}}, false},
		{"explicit set", "{'V100:1', 'A100:1'}", []taskyaml.Accelerator{one("V100"), one("A100")}, false},
		{"bare set", "{V100, A100}", []taskyaml.Accelerator{one("V100"), one("A100")}, false},
		{"YAML set tag", "!!set {'V100:1': null, 'A100:2': null}", []taskyaml.Accelerator{one("V100"), {Name: "A100", Count: taskyaml.Quantity{Value: "2"}}}, false},
		{"block set", "\n    ? V100:1\n    ? A100:1", []taskyaml.Accelerator{one("V100"), one("A100")}, false},
		{"null valued map", "{T4: null}", []taskyaml.Accelerator{one("T4")}, false},
		{"sequence", "[A100:8, 'V100:8', H100]", []taskyaml.Accelerator{{Name: "A100", Count: taskyaml.Quantity{Value: "8"}}, {Name: "V100", Count: taskyaml.Quantity{Value: "8"}}, one("H100")}, true},
		{"same model alternatives", "{L4:1, L4:2}", []taskyaml.Accelerator{one("L4"), {Name: "L4", Count: taskyaml.Quantity{Value: "2"}}}, false},
		{"empty map", "{}", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := taskyaml.Parse(strings.NewReader("resources:\n  accelerators: "+tt.yaml+"\n"), taskyaml.Options{})
			if err != nil {
				t.Fatal(err)
			}
			r := d.Task.Resources
			if !reflect.DeepEqual(r.Accelerators, tt.want) || r.AcceleratorsOrdered != tt.ordered {
				t.Fatalf("got %+v ordered %v, want %+v ordered %v", r.Accelerators, r.AcceleratorsOrdered, tt.want, tt.ordered)
			}
			if _, ok := d.Locations["resources.accelerators"]; !ok {
				t.Fatal("location lost")
			}
		})
	}
}

// TestResourceForms covers the upstream disk_size, infra, autostop and
// job_recovery forms (sky/utils/schemas.py).
func TestResourceForms(t *testing.T) {
	tests := []struct {
		name, yaml string
		want       taskyaml.Resources
	}{
		{"disk integer", "disk_size: 512", taskyaml.Resources{DiskSize: &taskyaml.Quantity{Value: "512"}}},
		{"disk unit", "disk_size: 2Ti", taskyaml.Resources{DiskSize: &taskyaml.Quantity{Value: "2", Unit: "Ti"}}},
		{"disk quoted default", "disk_size: '256'", taskyaml.Resources{DiskSize: &taskyaml.Quantity{Value: "256"}}},
		{"disk null", "disk_size: null", taskyaml.Resources{}},
		{"infra", "infra: k8s/my-context", taskyaml.Resources{Infra: str("k8s/my-context")}},
		{"infra null", "infra: null", taskyaml.Resources{}},
		{"autostop duration", "autostop: 10m", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(10)}}},
		{"autostop hours", "autostop: 2H", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(120)}}},
		{"autostop weeks", "autostop: 1w", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(10080)}}},
		{"autostop quoted minutes", "autostop: '15'", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(15)}}},
		{"autostop integer", "autostop: 5", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(5)}}},
		{"autostop negative integer", "autostop: -1", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(-1)}}},
		{"autostop false", "autostop: false", taskyaml.Resources{Autostop: &taskyaml.Autostop{}}},
		{"autostop true", "autostop: true", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true}}},
		{"autostop empty map", "autostop: {}", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true}}},
		{"autostop map", "autostop: {idle_minutes: 10, down: true, wait_for: JOBS}", taskyaml.Resources{Autostop: &taskyaml.Autostop{Enabled: true, IdleMinutes: i64(10), Down: boolp(true), WaitFor: str("JOBS")}}},
		{"autostop null", "autostop: null", taskyaml.Resources{}},
		{"recovery string", "job_recovery: FAILOVER", taskyaml.Resources{JobRecovery: &taskyaml.JobRecovery{Strategy: str("FAILOVER")}}},
		{"recovery map", "job_recovery: {strategy: EAGER_NEXT_REGION, max_restarts_on_errors: 3, recover_on_exit_codes: [33, 34]}", taskyaml.Resources{JobRecovery: &taskyaml.JobRecovery{Strategy: str("EAGER_NEXT_REGION"), MaxRestartsOnErrors: i64(3), RecoverOnExitCodes: []int{33, 34}}}},
		{"recovery one code", "job_recovery: {recover_on_exit_codes: 0}", taskyaml.Resources{JobRecovery: &taskyaml.JobRecovery{RecoverOnExitCodes: []int{0}}}},
		{"recovery null strategy", "job_recovery: {strategy: null}", taskyaml.Resources{JobRecovery: &taskyaml.JobRecovery{}}},
		{"recovery empty", "job_recovery: {}", taskyaml.Resources{JobRecovery: &taskyaml.JobRecovery{}}},
		{"recovery null", "job_recovery: null", taskyaml.Resources{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := taskyaml.Parse(strings.NewReader("resources:\n  "+tt.yaml+"\n"), taskyaml.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*d.Task.Resources, tt.want) {
				t.Fatalf("got %#v, want %#v", *d.Task.Resources, tt.want)
			}
			if len(d.Locations) != 2 {
				t.Fatalf("locations %v: present field lost", d.Locations)
			}
		})
	}
}

// TestResourceRefusals checks malformed and unknown resource forms fail
// with typed errors that echo no source text.
func TestResourceRefusals(t *testing.T) {
	tests := []struct {
		name, yaml string
		code       taskyaml.Code
		path       string
	}{
		{"accel zero", "accelerators: 's3cr3t:0'", taskyaml.Invalid, "resources.accelerators"},
		{"accel negative", "accelerators: {s3cr3t: -1}", taskyaml.Invalid, "resources.accelerators"},
		{"accel word count", "accelerators: 's3cr3t:many'", taskyaml.Invalid, "resources.accelerators"},
		{"accel string map count", "accelerators: {s3cr3t: '2'}", taskyaml.Invalid, "resources.accelerators"},
		{"accel bool count", "accelerators: {s3cr3t: true}", taskyaml.Invalid, "resources.accelerators"},
		{"accel nested count", "accelerators: {s3cr3t: [1]}", taskyaml.Invalid, "resources.accelerators"},
		{"set with value", "accelerators: !!set {s3cr3t: 2}", taskyaml.Invalid, "resources.accelerators"},
		{"set outside accelerators", "autostop: !!set {s3cr3t: null}", taskyaml.Invalid, "resources.autostop"},
		{"accel colon key with count", "accelerators: {'s3cr3t:1': 2}", taskyaml.Invalid, "resources.accelerators"},
		{"accel three parts", "accelerators: 'a:s3cr3t:1'", taskyaml.Invalid, "resources.accelerators"},
		{"accel empty name", "accelerators: ':1'", taskyaml.Invalid, "resources.accelerators"},
		{"accel padded name", "accelerators: {' s3cr3t': 1}", taskyaml.Invalid, "resources.accelerators"},
		{"accel minimum", "accelerators: 's3cr3t:2+'", taskyaml.Unsupported, "resources.accelerators"},
		{"accel memory", "accelerators: '80GB+:1'", taskyaml.Unsupported, "resources.accelerators"},
		{"accel maker memory", "accelerators: 'nvidia:24GB:1'", taskyaml.Unsupported, "resources.accelerators"},
		{"accel integer", "accelerators: 4", taskyaml.Invalid, "resources.accelerators"},
		{"accel sequence item", "accelerators: [{s3cr3t: 1}]", taskyaml.Invalid, "resources.accelerators"},
		{"accel duplicate set", "accelerators: {s3cr3t:1, 's3cr3t:1'}", taskyaml.Invalid, "resources.accelerators"},
		{"accel duplicate key", "accelerators: {s3cr3t: 1, s3cr3t: 1}", taskyaml.Invalid, "resources.accelerators"},
		{"accel duplicate list", "accelerators: [s3cr3t, 's3cr3t:1']", taskyaml.Invalid, "resources.accelerators"},
		{"disk fraction", "disk_size: 1.5", taskyaml.Invalid, "resources.disk_size"},
		{"disk zero", "disk_size: 0", taskyaml.Invalid, "resources.disk_size"},
		{"disk negative", "disk_size: -10", taskyaml.Invalid, "resources.disk_size"},
		{"disk ratio", "disk_size: 2x", taskyaml.Invalid, "resources.disk_size"},
		{"disk word", "disk_size: s3cr3t", taskyaml.Invalid, "resources.disk_size"},
		{"disk map", "disk_size: {s3cr3t: 1}", taskyaml.Invalid, "resources.disk_size"},
		{"infra number", "infra: 7", taskyaml.Invalid, "resources.infra"},
		{"infra list", "infra: [s3cr3t]", taskyaml.Invalid, "resources.infra"},
		{"autostop word", "autostop: s3cr3t", taskyaml.Invalid, "resources.autostop"},
		{"autostop seconds", "autostop: 30s", taskyaml.Invalid, "resources.autostop"},
		{"autostop float", "autostop: 1.5", taskyaml.Invalid, "resources.autostop"},
		{"autostop overflow", "autostop: 9223372036854775807w", taskyaml.Invalid, "resources.autostop"},
		{"autostop list", "autostop: [10]", taskyaml.Invalid, "resources.autostop"},
		{"autostop unknown key", "autostop: {s3cr3t: 1}", taskyaml.Invalid, "resources.autostop"},
		{"autostop negative idle", "autostop: {idle_minutes: -1}", taskyaml.Invalid, "resources.autostop"},
		{"autostop string idle", "autostop: {idle_minutes: 10m}", taskyaml.Invalid, "resources.autostop"},
		{"autostop string down", "autostop: {down: s3cr3t}", taskyaml.Invalid, "resources.autostop"},
		{"autostop null down", "autostop: {down: null}", taskyaml.Invalid, "resources.autostop"},
		{"autostop wait mode", "autostop: {wait_for: s3cr3t}", taskyaml.Invalid, "resources.autostop"},
		{"autostop hook", "autostop: {hook: s3cr3t}", taskyaml.Unsupported, "resources.autostop"},
		{"autostop hook timeout", "autostop: {hook_timeout: 5}", taskyaml.Unsupported, "resources.autostop"},
		{"recovery unknown key", "job_recovery: {s3cr3t: 1}", taskyaml.Unsupported, "resources.job_recovery"},
		{"recovery number", "job_recovery: 3", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery list", "job_recovery: [s3cr3t]", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery strategy type", "job_recovery: {strategy: [s3cr3t]}", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery negative restarts", "job_recovery: {max_restarts_on_errors: -1}", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery string restarts", "job_recovery: {max_restarts_on_errors: s3cr3t}", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery code range", "job_recovery: {recover_on_exit_codes: 256}", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery code type", "job_recovery: {recover_on_exit_codes: [s3cr3t]}", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery duplicate code", "job_recovery: {recover_on_exit_codes: [1, 1]}", taskyaml.Invalid, "resources.job_recovery"},
		{"recovery nested list", "job_recovery: {recover_on_exit_codes: [[1]]}", taskyaml.Invalid, "resources.job_recovery"},
		{"use_spot string", "use_spot: s3cr3t", taskyaml.Invalid, "resources.use_spot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := taskyaml.Parse(strings.NewReader("resources:\n  "+tt.yaml+"\n"), taskyaml.Options{})
			var pe *taskyaml.Error
			if !errors.As(err, &pe) || d != nil {
				t.Fatalf("got %v, %v; want typed error", d, err)
			}
			if pe.Code != tt.code || pe.Path != tt.path || pe.Line != 2 {
				t.Fatalf("got %+v, want %s %s on line 2", pe, tt.code, tt.path)
			}
			if strings.Contains(err.Error(), "s3cr3t") {
				t.Fatal("input leaked")
			}
		})
	}
}
